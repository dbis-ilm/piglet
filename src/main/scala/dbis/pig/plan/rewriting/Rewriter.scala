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

import dbis.pig.op.{And, Filter, Load, Materialize, OrderBy, PigOperator, Pipe, Store, _}
import dbis.pig.plan.{DataflowPlan, MaterializationManager}
import dbis.pig.tools.BreadthFirstBottomUpWalker
import org.kiama.rewriting.Rewriter._
import org.kiama.rewriting.Strategy
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.{ClassTag, classTag}
import com.typesafe.scalalogging.LazyLogging
import java.net.URI

object Rewriter extends LazyLogging {
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
  private def processPigOperator(sink: PigOperator, strategy: Strategy): PigOperator = {
    val rewriter = bottomup(attempt(strategy))
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
        newPlanNodes --= source.outputs.flatMap(_.consumer)

        newPlanNodes += source
        for (output <- source.outputs.flatMap(_.consumer)) {
          // We've found a new node - it needs to be included in the new plan, so add it to the new plans nodes.
          newPlanNodes += output
          // And we need to process its output nodes in the future.
          // If we already processed a nodes outputs, they'll be removed again and put at the head of the new plans list
          // of operators.
          nodesToProcess ++= output.outputs.flatMap(_.consumer)
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
  private def mergeFilters(parent: Filter, child: Filter): Option[PigOperator] =
    Some(Filter(child.out, parent.in, And(parent.pred, child.pred)))

  private def splitIntoToFilters(node: Any): Option[List[PigOperator]] = node match {
    case node@SplitInto(inPipeName, splits) =>
      val filters = (for (branch <- splits) yield branch.output.name -> Filter(branch.output, inPipeName, branch
        .expr)).toMap
      node.inputs = node.inputs.map(p => {
        p.consumer = p.consumer.filterNot(_ == node)
        p
      })
      // For all outputs
      node.outputs.iterator foreach (_.consumer.foreach(output => {
        // Iterate over their inputs
        output.inputs foreach (input => {
          // Check if the relation name is one of the names our SplitBranches write
          if (filters contains input.name) {
            // Replace SplitInto with the appropriate Filter
            output.inputs = output.inputs.filter(_.producer != node) :+ Pipe(input.name, filters(input.name))
            filters(input.name).inputs = node.inputs
          }
        })
      }
      ))
      Some(filters.values.toList)
    case _ => None
  }

  /** Replaces sink nodes that are not [[dbis.pig.op.Store]] operators with [[dbis.pig.op.Empty]] ones.
    *
    * @param node
    * @return
    */
  //noinspection ScalaDocMissingParameterDescription
  private def removeNonStorageSinks(node: Any): Option[PigOperator] = node match {
    // Store and Dump are ok
    case Store(_, _, _) => None
    case Dump(_) => None
    // To prevent recursion, empty is ok as well
    case Empty(_) => None
    case op: PigOperator =>
      op.outputs match {
      case Pipe(_, _, Nil) :: Nil =>
        val newNode = Empty(Pipe(""))
        // Set outputs *again*. Passing a Pipe("") to the constructor causes problems because other parts of the code
        // expect Pipe.consumer and Pipe.producer to be non-null, but we can't set Pipe.producer without the new node.
        newNode.outputs = List(Pipe("", newNode, List.empty))
        newNode.inputs = op.inputs
        Some(newNode)
      case _ => None
    }
    case _ => None
  }

  /** If an operator is followed by an Empty node, replace it with the Empty node
    *
    * @param parent
    * @param child
    * @return
    */
  //noinspection ScalaDocMissingParameterDescription
  private def mergeWithEmpty(parent: PigOperator, child: Empty): Option[PigOperator] = Some(child)

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
    val strategy = (parent: T, child: T2) =>
      f(parent, child).map(fixInputsAndOutputs(parent, child, _))
    addBinaryPigOperatorStrategy(strategy)
  }

  /** Add a new strategy for reordering two operators.
    *
    * A new reordering strategy can be added to the rewriter via
    * {{{
    *  reorder[OrderBy, Filter]
    * }}}
    *
    * @tparam T The type of the parent operator.
    * @tparam T2 The type of the child operator.
    */
  def reorder[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag]():
  Unit = {
    val strategy = (parent: T, child: T2) =>
      Some(fixInputsAndOutputs(parent, child, child, parent))
    addBinaryPigOperatorStrategy(strategy)
  }

  ///////////
  // insertAfter, replace, remove and swap are methods that usually do not get called directly, but indirectly
  // through their counterparts on DataflowPLan
  ///////////

  /** Insert `newOp` after `old` in `plan`.
    *
    * @param plan
    * @param old
    * @param newOp
    * @return
    */
  //noinspection ScalaDocMissingParameterDescription
  def insertAfter(plan: DataflowPlan, old: PigOperator, newOp: PigOperator): DataflowPlan = {
    require(!newOp.isInstanceOf[Load], "Can't insert a Load operator after another operator")
    require((old.outPipeName == "") || (old.outPipeName == newOp.inputs.head.name), "The new operator has to read " +
      "from " +
      "the old one")
    val strategy = (op: Any) => {
      if (op == old){
        val outIdx = old.outputs.indexWhere(_.name == newOp.inputs.head.name)
        if (outIdx > -1) {
          // newOp doesn't read from old - in practice, this can only happen if old's outPipeName is "", which means
          // it simply does not yet have any outputs
          old.outputs(outIdx).consumer = old.outputs(outIdx).consumer :+ newOp
        } else {
          old.outputs :+ List(Pipe(newOp.inputs.head.name, old, List(newOp)))
        }
        newOp.inputs.head.producer = old
        Some(op)
      }
      else {
        None
      }
    }
    processPlan(plan, strategyf(t => strategy(t)))
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
        repl.outputs = old.outputs
        repl.outputs = old.outputs
        Some(repl)
      }
      else {
        None
      }
    }
    processPlan(plan, strategyf(t => strategy(t)))
  }

  /** Removes `rem` from `plan`.
    *
    * If `rem` has any child nodes in the plan, they will take its place.
    *
    * @param plan
    * @param rem
    * @return A new [[dbis.pig.plan.DataflowPlan]] without `rem`.
    */
  //noinspection ScalaDocMissingParameterDescription
  def remove(plan: DataflowPlan, rem: PigOperator): DataflowPlan = {
    val strategy = (op: Any) => {
      if (op == rem) {
        val pigOp = op.asInstanceOf[PigOperator]
        val newOps = pigOp.outputs.flatMap(_.consumer).map((inOp: PigOperator) => {
          // Remove input pipes to `op` and replace them with `ops` input pipes
          inOp.inputs = inOp.inputs.filterNot(_.producer == pigOp) ++ pigOp.inputs
          inOp
        })
        // Replace `op` in its inputs output pipes with `ops` children
        pigOp.inputs.map(_.producer).foreach(_.outputs.foreach((out: Pipe) => {
          if (out.consumer contains op) {
            out.consumer = out.consumer.filterNot(_ == op) ++ newOps
          }
        }))
        Some(newOps)
      }
      else {
        None
      }
    }
    processPlan(plan, strategyf(t => strategy(t)))
  }

  /** Swap the positions of `op1` and `op2` in `plan`
    *
    * @param plan
    * @param op1 The parent and new child operator.
    * @param op2 The child and new parent operator.
    * @return
    */
  //noinspection ScalaDocMissingParameterDescription
  def swap(plan: DataflowPlan, op1: PigOperator, op2: PigOperator): DataflowPlan = {
    val strategy = (parent: PigOperator, child: PigOperator) => {
      if (parent == op1 && child == op2) {
        val np = fixInputsAndOutputs(parent, child, child, parent)
        Some(np)
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
          if (parent.outputs.length == 1 && parent.outputs.head.consumer.length == 1) {
            val op2 = parent.outputs.head.consumer.head
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
    newParent.outputs = oldChild.outputs

    // Each Operator that has oldChild in its inputs list as a producer needs to have it replaced with newParent
    oldChild.outputs foreach { output =>
      output.consumer foreach { op =>
        op.inputs = op.inputs.filter(_.producer != oldChild) :+ Pipe(newParent.outPipeName, newParent, List(op))
      }
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
    // If oldParent == newChild (for example when this is called from `swap`, we need to save oldParent.outPipename
    // because it depends on oldParent.outputs
    val oldparent_outpipename = oldParent.outPipeName

    // See above, of oldParent == newChild, we need to use oldParent.inputs while we can
    newParent.inputs = oldParent.inputs

    newChild.inputs = List(Pipe(newParent.outPipeName, newParent, List(newChild)))
    newChild.outputs = oldChild.outputs

    newParent.outputs = List(Pipe(oldparent_outpipename, newParent, List(newChild)))

    // Each Operator that has oldChild in its inputs list as a producer needs to have it replaced with newChild
    oldChild.outputs foreach { output =>
      output.consumer foreach { op =>
        op.inputs = op.inputs.filter(_.producer != oldChild) :+ Pipe(newParent.outPipeName, newChild, List(op))
      }
    }

    // Replacing oldParent with newParent in oldParents inputs outputs list is done by kiamas Rewritable trait
    newParent
  }

  def processMaterializations(plan: DataflowPlan, mm: MaterializationManager): DataflowPlan = {
    require(plan != null, "Plan must not be null")
    require(mm != null, "Materialization Manager must not be null")

    val walker = new BreadthFirstBottomUpWalker

    val materializes = ListBuffer.empty[Materialize]

    walker.walk(plan) { op =>
      op match {
        case o: Materialize => materializes += o
        case _ =>
      }
    }

    logger.debug(s"found ${materializes.size} materialize operators")
    
    var newPlan = plan

    /* we should check here if the op is still connected to a sink
     * the ops will all still be in the plan, but they might be disconnected
     * if a load was inserted before
     */
    for (materialize <- materializes if plan.containsOperator(materialize)) {

      val data = mm.getDataFor(materialize.lineageSignature)

      /*
       * The materialization manager has data for the current materialization 
       * operator. So create a new Load operator for the materialized result 
       * and add it to the plan by replacing the input of the Materialize-Op
       * with the loader.
       */
      if(data.isDefined) {
        logger.debug(s"found materialized data for materialize operator $materialize")
        
        val loader = Load(materialize.inputs.head, new URI(data.get), materialize.constructSchema, "BinStorage")
        val matInput = materialize.inputs.head.producer

        for (inPipe <- matInput.inputs) {
          plan.disconnect(inPipe.producer, matInput)
        }

        newPlan = plan.replace(matInput, loader)

        logger.info(s"replaced materialize op with loader $loader")
        
        /* TODO: do we need to remove all other nodes that get disconnected now by hand
         * or do they get removed during code generation (because there is no sink?)
         */
        newPlan = newPlan.remove(materialize)

      } else {
        /* there is a MATERIALIZE operator, for which no results could be found
         * --> store them by adding a STORE operator to the MATERIALIZE operator's input op
         * then, remove the materialize op
         */
        logger.debug(s"did not find materialized data for materialize operator $materialize")
        
        val file = mm.saveMapping(materialize.lineageSignature)
        val storer = new Store(materialize.inputs.head, new URI(file), "BinStorage")

        newPlan = plan.insertAfter(materialize.inputs.head.producer, storer)
        newPlan = newPlan.remove(materialize)
        
        logger.info(s"inserted new store operator $storer")
      }
    }

    newPlan
  }

  merge[Filter, Filter](mergeFilters)
  merge[PigOperator, Empty](mergeWithEmpty)
  reorder[OrderBy, Filter]
  addStrategy(strategyf(t => splitIntoToFilters(t)))
  addStrategy(removeNonStorageSinks _)
}
