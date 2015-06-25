/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dbis.pig.plan.rewriting

import dbis.pig.op.{And, Filter, OrderBy, PigOperator}
import dbis.pig.plan.{DataflowPlan, Pipe}
import org.kiama.rewriting.Rewriter._
import org.kiama.rewriting.Strategy

import scala.collection.mutable.LinkedHashSet

object Rewriter {
  private var strategy = fail

  /** Add a [[org.kiama.rewriting.Strategy]] to this Rewriter.
   *
   * It will be added by [[org.kiama.rewriting.Rewriter.ior]]ing it with the already existing ones.
   * @param s The new strategy.
   */
  def addStrategy(s: Strategy): Unit = {
    strategy = ior(strategy, s)
  }

  def addStrategy(f: Any => Option[PigOperator]): Unit = addStrategy(strategyf(t => f(t)))

  /** Rewrites a given sink node with several [[org.kiama.rewriting.Strategy]]s that were added via
   * [[dbis.pig.plan.rewriting.Rewriter.addStrategy]].
   *
   * @param sink The sink node to rewrite.
   * @return The rewritten sink node.
   */
  def processSink(sink: PigOperator): PigOperator = {
    val rewriter = bottomup( attempt (strategy))
    rewrite(rewriter)(sink)
  }

  /** Apply all rewriting rules of this Rewriter to a [[dbis.pig.plan.DataflowPlan]].
    *
    * @param plan
    * @return A rewritten [[dbis.pig.plan.DataflowPlan]]
    */
  def processPlan(plan: DataflowPlan): DataflowPlan = {
    // This looks innocent, but this is where the rewriting happens.
    val newSinks = plan.sinkNodes.map(processSink)

    var newPlanNodes = LinkedHashSet[PigOperator]() ++= newSinks
    var nodesToProcess = newSinks.toList

    // We can't modify nodesToProcess while iterating over it. Therefore we'll iterate over a copy of it as long as
    // it contains elements.
    while (nodesToProcess.length > 0) {
      val iter = nodesToProcess.iterator
      nodesToProcess = List[PigOperator]()
      for (sink <- iter) {
        // newPlanNodes might already contain this PigOperator, but we encountered it again. Remove it to later add it
        // again, thereby "pushing" it to an earlier position in the new plans list of operators because a
        // LinkedHashSet iterates over the elements in the order of insertion and we later *reverse* the the whole
        // thing, so PigOperators inserted later get emitted first.
        // This is to make sure that that sink is emitted before all other operators that need its data.
        newPlanNodes -= sink
        // And remove its inputs as well to revisit them later on.
        newPlanNodes --= sink.inputs.map(_.producer)

        newPlanNodes += sink
        for (input <- sink.inputs) {
          val producer = input.producer
          // We've found a new node - it needs to be included in the new plan, so add it to the new plans nodes.
          newPlanNodes += producer
          // And we need to process its input nodes in the future.
          // If we already processed a nodes input, they'll be removed again and put at the head of the new plans list
          // of operators.
          nodesToProcess ++= producer.inputs.map(_.producer)
        }
      }
    }

    var newPlan = new DataflowPlan(newPlanNodes.toList.reverse)
    newPlan.additionalJars ++= plan.additionalJars
    newPlan
  }

  /** Merges two [[dbis.pig.op.Filter]] operations if one is the only input of the other.
   *
   * @param pigOperator A [[dbis.pig.op.Filter]] operator whose only input is another Filter.
   * @return On success, an Option containing a new [[dbis.pig.op.Filter]] operator with the predicates of both input
   *         Filters, None otherwise.
   */
  private def mergeFilters(pigOperator: Any): Option[Filter] = pigOperator match {
    case f1 @ Filter(out, _, predicate) =>
      f1.inputs match {
        case List((Pipe(_, f2 @ Filter(_, in, predicate2)))) =>
          val newFilter = Filter(out, in, And(predicate, predicate2))
          // TODO extract merging PigOperators into a new method, possibly on PigOperator itself
          // Use the second filters inputs. We don't need to rewrite the inputs output because that always seems to be
          // pointing to the input itself?
          newFilter.inputs = f2.inputs
          // TODO can there be other objects that have f1 in their input list?
          Some(newFilter)
        case _ => None
      }
    case _ => None
  }

  /** Puts [[dbis.pig.op.Filter]] operators before [[dbis.pig.op.OrderBy]] ones.
   *
   * @param pigOperator A [[dbis.pig.op.Filter]] operator whose only input is a [[dbis.pig.op.OrderBy]] operator.
   * @return On success, an Option containing a new [[dbis.pig.op.OrderBy]] operators whose input is the
   *         [[dbis.pig.op.Filter]] passed into this method, None otherwise.
   */
  private def filterBeforeOrder(pigOperator: Any): Option[OrderBy] = pigOperator match {
    case f @ Filter(out, in, predicate) =>
      f.inputs match {
        case List((Pipe(_, order @ OrderBy(out2, in2, orderSpec)))) =>
          // Reorder the operations and swap their input and output names
          val newOrder = order.copy(out, in, orderSpec)
          val newFilter = f.copy(out2, in2, predicate)

          newOrder.inputs = List(Pipe(in, newFilter))

          newFilter.inputs = order.inputs

          Some(newOrder)
        case _ => None
      }
    case _ => None
  }

  addStrategy(mergeFilters _)
  addStrategy(filterBeforeOrder _)
}
