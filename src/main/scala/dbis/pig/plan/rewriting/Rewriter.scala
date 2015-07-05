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

import scala.collection.mutable
import scala.reflect.{ClassTag, classTag}

object Rewriter {
  private var ourStrategy = fail

  /** Add a [[org.kiama.rewriting.Strategy]] to this Rewriter.
   *
   * It will be added by [[org.kiama.rewriting.Rewriter.ior]]ing it with the already existing ones.
   * @param s The new strategy.
   */
  def addStrategy(s: Strategy): Unit = {
    ourStrategy = ior(ourStrategy, s)
  }

  def addStrategy(f: Any => Option[PigOperator]): Unit = addStrategy(strategyf(t => f(t)))

  /** Rewrites a given sink node with several [[org.kiama.rewriting.Strategy]]s that were added via
   * [[dbis.pig.plan.rewriting.Rewriter.addStrategy]].
   *
   * @param sink The sink node to rewrite.
   * @return The rewritten sink node.
   */
  def processPigOperator(sink: PigOperator): PigOperator = {
    processPigOperator(sink, ourStrategy)
  }

  /** Process a sink with a specified strategy
    *
    * @param sink The sink to process.
    * @param strategy The strategy to apply.
    * @return
    */
  def processPigOperator(sink: PigOperator, strategy: Strategy): PigOperator = {
    val rewriter = bottomup( attempt (strategy))
    rewrite(rewriter)(sink)
  }

  /** Apply all rewriting rules of this Rewriter to a [[dbis.pig.plan.DataflowPlan]].
    *
    * @param plan The plan to process.
    * @return A rewritten [[dbis.pig.plan.DataflowPlan]]
    */
  def processPlan(plan: DataflowPlan): DataflowPlan = processPlan(plan, ourStrategy)

  def processPlan(plan: DataflowPlan, strategy: Strategy): DataflowPlan = {
    // This looks innocent, but this is where the rewriting happens.
    val newSources = plan.sourceNodes.map(processPigOperator(_, strategy))

    var newPlanNodes = mutable.LinkedHashSet[PigOperator]() ++= newSources
    var nodesToProcess = newSources.toList

    // We can't modify nodesToProcess while iterating over it. Therefore we'll iterate over a copy of it as long as
    // it contains elements.
    while (nodesToProcess.nonEmpty) {
      val iter = nodesToProcess.iterator
      nodesToProcess = List[PigOperator]()
      for (source <- iter) {
        // newPlanNodes might already contain this PigOperator, but we encountered it again. Remove it to later add it
        // again, thereby "pushing" it to an earlier position in the new plans list of operators because a
        // LinkedHashSet iterates over the elements in the order of insertion, so PigOperators inserted later get
        // emitted first.
        // This is to make sure that that source is emitted before all other operators that need its data.
        newPlanNodes -= source
        // And remove its outputs as well to revisit them later on.
        newPlanNodes --= source.outputs

        newPlanNodes += source
        for (output <- source.outputs) {
          // We've found a new node - it needs to be included in the new plan, so add it to the new plans nodes.
          newPlanNodes += output
          // And we need to process its output nodes in the future.
          // If we already processed a nodes outputs, they'll be removed again and put at the head of the new plans list
          // of operators.
          nodesToProcess ++= output.outputs
        }
      }
    }

    val newPlan = new DataflowPlan(newPlanNodes.toList)
    newPlan.additionalJars ++= plan.additionalJars
    newPlan
  }

  /** Merges two [[dbis.pig.op.Filter]] operations if one is the only input of the other.
    *
    * @param parent The parent filter.
    * @param child The child filter.
    * @return On success, an Option containing a new [[dbis.pig.op.Filter]] operator with the predicates of both input
    *         Filters, None otherwise.
    */
  private def mergeFilters(parent: Filter, child: Filter): Option[PigOperator] = {
    Some(Filter(child.output.get, parent.initialInPipeName, And(parent.pred, child.pred)))
  }

  /** Puts [[dbis.pig.op.Filter]] operators before [[dbis.pig.op.OrderBy]] ones.
    *
    * @param parent The parent operator, in this case, a [[dbis.pig.op.Filter]] object.
    * @param child The child operator, in this case, a [[dbis.pig.op.OrderBy]] object.
    * @return On success, an Option containing a new [[dbis.pig.op.OrderBy]] operators whose input is the
    *         [[dbis.pig.op.Filter]] passed into this method, None otherwise.
    */
  private def filterBeforeOrder(parent: OrderBy, child: Filter): Option[(Filter, OrderBy)] = {
    val newOrder = parent.copy(child.initialOutPipeName, child.initialInPipeName, parent.orderSpec)
    val newFilter = child.copy(parent.initialOutPipeName, parent.initialInPipeName, child.pred)
    Some((newFilter, newOrder))
  }


  /** Add a new strategy for merging operators of two types.
    *
    * An example method to merge Filter operators is
    * {{{
    *  def mergeFilters(parent: Filter, child: Filter): Option[PigOperator] = {
    *    Some(Filter(child.output.get, parent.initialInPipeName, And(parent.pred, child.pred)))
    *  }
    * }}}
    *
    * It can be added to the rewriter via
    * {{{
    *  merge[Filter, Filter](mergeFilters)
    * }}}
    *
    * @param f The function to perform the merge. It does not have to modify inputs and outputs, this will be done
    *          automatically.
    * @tparam T The type of the first operator.
    * @tparam T2 The type of the second operator.
    */
  def merge[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag](f: (T, T2) => Option[PigOperator]):
  Unit = {
    val strategy = (parent: T, child: T2) => {
      val result = f(parent, child)
      result.map(fixInputsAndOutputs(parent, child, _))
    }
    addBinaryPigOperatorStrategy(strategy)
  }

  /** Add a new strategy for reordering two operators.
    *
    * An example method to reorder Filter operators before OrderBy ones is
    * {{{
    * def filterBeforeOrder(parent: OrderBy, child: Filter): Option[(Filter, OrderBy)] = {
    *   val newOrder = parent.copy(child.initialOutPipeName, child.initialInPipeName, parent.orderSpec)
    *   val newFilter = child.copy(parent.initialOutPipeName, parent.initialInPipeName, child.pred)
    *   Some((newFilter, newOrder))
    * }
    * }}}
    *
    * It can be added to the rewriter via
    * {{{
    *  reorder[OrderBy, Filter](filterBeforeOrder)
    * }}}
    *
    * @param f The function to perform the reordering. It does not have to modify inputs and outputs, this will be
    *          done automatically.
    * @tparam T The type of the parent operator.
    * @tparam T2 The type of the child operator.
    */
  def reorder[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag](f: (T, T2) => Option[(T2, T)]):
  Unit = {
    val strategy = (parent: T, child: T2) => {
      val result = f(parent, child)
      result.map(tup => fixInputsAndOutputs(parent, tup._1, child, tup._2))
    }
    addBinaryPigOperatorStrategy(strategy)
  }

  /** Replace `old` with `repl` in `plan`.
    *
    * @param plan
    * @param old
    * @param repl
    * @return A new [[dbis.pig.plan.DataflowPlan]] in which `old` has been replaced with `repl`.
    */
  //noinspection ScalaDocMissingParameterDescription
  def replace(plan: DataflowPlan, old: PigOperator, repl: PigOperator): DataflowPlan = {
    val strategy = (op: Any) => {
      if (op == old) {
        repl.inputs = old.inputs
        repl.output = old.output
        repl.outputs = old.outputs
        Some(repl)
      }
      else {
        None
      }
    }
    processPlan(plan, strategyf(t => strategy(t)))
  }

  /** Remove `rem` from `plan`
    *
    * @param plan
    * @param rem
    * @return A new [[dbis.pig.plan.DataflowPlan]] without `rem`.
    */
  //noinspection ScalaDocMissingParameterDescription
  def remove(plan: DataflowPlan, rem: PigOperator): DataflowPlan = {
    val strategy = (parent: PigOperator, child: PigOperator) => {
      if (child == rem) {
        parent.outputs = parent.outputs.filter(_ != child)
        Some(fixInputsAndOutputs(parent, child, parent))
      }
      else {
        None
      }
    }
    processPlan(plan, buildBinaryPigOperatorStrategy(strategy))
  }

  /** Add a strategy that applies a function to two operators.
    *
    * @param f The function to apply.
    * @tparam T2 The second operators type.
    * @tparam T The first operators type.
    */
  private def addBinaryPigOperatorStrategy[T2 <: PigOperator : ClassTag, T <: PigOperator : ClassTag](f: (T, T2)
    => Option[PigOperator]): Unit = {
    val strategy = buildBinaryPigOperatorStrategy(f)
    addStrategy(strategy)
  }

  private def buildBinaryPigOperatorStrategy[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag]
    (f: (T, T2) => Option[PigOperator]): Strategy = {
    strategyf(op => {
      op match {
        case `op` if classTag[T].runtimeClass.isInstance(op) =>
          val parent = op.asInstanceOf[T]
          if (parent.outputs.length == 1) {
            val op2 = parent.outputs.head
            op2 match {
              case `op2` if classTag[T2].runtimeClass.isInstance(op2) && op2.inputs.length == 1 =>
                val child = op2.asInstanceOf[T2]
                f(parent, child)
              case _ => None
            }
          }
          else {
            None
          }
        case _ => None
      }
    })
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
    newParent.inputs = oldParent.inputs
    newParent.output = oldChild.output
    newParent.outputs = oldChild.outputs

    // Replace oldChild in its outputs inputs attribute with newParent
    for(out <- oldChild.outputs) {
      out.inputs = out.inputs.filter(_.producer != oldChild) :+ Pipe(newParent.output.get, newParent)
    }

    // Replacing oldParent with newParent in the outputs attribute of oldParents inputs producers is done by kiamas
    // Rewritable trait
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
    newChild.inputs = List(Pipe(newParent.output.get, newParent))
    newChild.output = oldChild.output
    newChild.outputs = oldChild.outputs

    newParent.inputs = oldParent.inputs
    newParent.output = oldParent.output
    newParent.outputs = List(newChild)

    // Replace oldChild in its outputs inputs attribute with newChild
    for(out <- oldChild.outputs) {
      out.inputs = out.inputs.filter(_.producer != oldChild)
      out.inputs = out.inputs :+ Pipe(newChild.output.get, newChild)
    }

    // Replacing oldParent with newParent in oldParents inputs outputs list is done by kiamas Rewritable trait
    newParent
  }

  merge[Filter, Filter](mergeFilters)
  reorder[OrderBy, Filter](filterBeforeOrder)
}
