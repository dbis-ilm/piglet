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
import scala.reflect.{ClassTag, classTag}

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
  private def mergeFilters(parent: Filter, child: Filter): Option[PigOperator] = {
    val newFilter = Filter(parent.output.get, child.initialInPipeName, And(parent.pred, child.pred))
    Some(newFilter)
  }

  /** Puts [[dbis.pig.op.Filter]] operators before [[dbis.pig.op.OrderBy]] ones.
   *
   * @param pigOperator A [[dbis.pig.op.Filter]] operator whose only input is a [[dbis.pig.op.OrderBy]] operator.
   * @return On success, an Option containing a new [[dbis.pig.op.OrderBy]] operators whose input is the
   *         [[dbis.pig.op.Filter]] passed into this method, None otherwise.
   */
  private def filterBeforeOrder(parent: Filter, child: OrderBy): Option[(OrderBy, Filter)] = {
    val newOrder = child.copy(parent.initialOutPipeName, parent.initialInPipeName, child.orderSpec)
    val newFilter = parent.copy(child.initialOutPipeName, child.initialInPipeName, parent.pred)
    Some((newOrder, newFilter))
  }


  /** Add a new strategy for merging operators of two types.
    *
    * @param f The function to perform the merge. It does not have to modify inputs and outputs, this will be done
    *          automatically.
    * @tparam T The type of the first operator.
    * @tparam T2 The type of the second operator.
    */
  def merge[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag](f: Function2[T, T2, Option[PigOperator]]):
  Unit = {
    val strategy = (parent: T, child: T2) => {
      val result = f(parent, child)
      result.map(fixInputsAndOutputs(parent, child, _))
    }
    addBinaryPigOperatorStrategy(strategy)
  }

  /** Add a new strategy for reordering two operators.
    *
    * @param f The function to perform the reordering. It does not have to modify inputs and outputs, this will be
    *          done automatically.
    * @tparam T The type of the first operator.
    * @tparam T2 The type of the second operator.
    */
  def reorder[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag](f: Function2[T, T2, Option[(T2, T)]]):
  Unit = {
    val strategy = (parent: T, child: T2) => {
      val result = f(parent, child)
      result.map(tup => fixInputsAndOutputs(parent, tup._1, child, tup._2))
    }
    addBinaryPigOperatorStrategy(strategy)
  }

  /** Add a strategy that applies a function to two operators.
    *
    * @param f The function to apply.
    * @tparam T2 The second operators type.
    * @tparam T The first operators type.
    */
  private def addBinaryPigOperatorStrategy[T2 <: PigOperator : ClassTag, T <: PigOperator : ClassTag](f: (T, T2)
    => Option[PigOperator]): Unit = {
    val strategy = (op: Any) => {
      op match {
        case op if classTag[T].runtimeClass.isInstance(op) => {
          val parent = op.asInstanceOf[T]
          if (parent.inputs.length == 1) {
            val op2 = parent.inputs.head.producer
            op2 match {
              case op2 if classTag[T2].runtimeClass.isInstance(op2) && op2.outputs.length == 1 => {
                val child = op2.asInstanceOf[T2]
                f(parent, child)
              }
              case _ => None
            }
          }
          else {
            None
          }
        }
        case _ => None
      }
    }
    addStrategy(strategy)
  }

  /** Fix the inputs and outputs attributes of PigOperators after an operation merged two of them into one.
    *
    * @param oldParent The old parent operator.
    * @param oldChild The old child operator.
    * @param newParent The new operator.
    * @tparam T The type of the old parent operator.
    * @tparam T2 The type of the old child operator.
    * @tparam T3 The type of the new operator.
    * @return
    */
  private def fixInputsAndOutputs[T <: PigOperator, T2 <: PigOperator, T3 <: PigOperator](oldParent: T, oldChild: T2,
                                                                                          newParent: T3): T3 = {
    newParent.inputs = oldChild.inputs
    newParent.output = oldParent.output
    newParent.outputs = oldParent.outputs

    // Fix replace oldChild in its inputs outputs attribute with newParent
    for(out <- oldChild.inputs) {
      val op = out.producer
      op.outputs = op.outputs.filter(_ != oldChild) :+ newParent
    }

    // Replacing oldParent with newParent in oldParents input list is done via kiamas Rewritable trait
    newParent
  }

  /** Fix the inputs and outputs attributes of PigOperators after two of them have been reordered.
    *
    * @param oldParent The old parent operator.
    * @param newParent The new parent operator.
    * @param oldChild The old child operator.
    * @param newChild The new child Operator.
    * @tparam T The type of the old parent and new child operators.
    * @tparam T2 The type of the old child and new parent operators.
    * @return
    */
  private def fixInputsAndOutputs[T <: PigOperator, T2 <: PigOperator](oldParent: T, newParent: T2, oldChild: T2,
                                                                       newChild: T): T2 = {
    newChild.inputs = oldChild.inputs
    newChild.output = oldChild.output
    newChild.outputs = oldChild.outputs

    newParent.inputs = List(Pipe(newChild.output.get, newChild))
    newParent.output = oldParent.output
    newParent.outputs = oldParent.outputs

    // Fix replace oldChild in its inputs outputs attribute with newChild
    for(out <- oldChild.inputs) {
      val op = out.producer
      op.outputs = op.outputs.filter(_ != oldChild) :+ newChild
    }

    // Replacing oldParent with newParent in oldParents input list is done via kiamas Rewritable trait
    newParent
  }

  merge[Filter, Filter](mergeFilters)
  reorder[Filter, OrderBy](filterBeforeOrder)
}
