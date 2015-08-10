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

import dbis.pig.op.{And, BinaryExpr, Filter, Load, Materialize, OrderBy, PigOperator, Pipe, Store, _}
import dbis.pig.plan.{DataflowPlan, MaterializationManager}
import dbis.pig.tools.BreadthFirstBottomUpWalker
import dbis.pig.plan.rewriting.Rules.registerAllRules
import dbis.pig.tools.BreadthFirstTopDownWalker
import scala.collection.mutable.Queue
import org.kiama.rewriting.Rewriter._
import org.kiama.rewriting.Strategy
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.{ClassTag, classTag}
import com.typesafe.scalalogging.LazyLogging
import java.net.URI

case class RewriterException(msg:String)  extends Exception(msg)

/** Provides various methods for rewriting [[DataflowPlan]]s by wrapping functionality provided by
  * Kiamas [[org.kiama.rewriting.Rewriter]] and [[org.kiama.rewriting.Strategy]] objects.
  *
  * This object keeps an internal [[Strategy]] object that holds all strategies added via [[addStrategy]]. Calling
  * [[processPlan]] with a [[DataflowPlan]] will apply ```all``` those strategies to the plan until none applies
  * anymore.
  *
  * ==Low-level methods==
  *
  * Methods that directly handle Strategies are provided:
  *
  *  - [[addStrategy]] adds a single strategy to this object. Later calls to [[processPlan]] will then use this
  *    strategy in addition to all other ones added via this method.
  *
  *  - [[addBinaryPigOperatorStrategy]] turns a function operating on two operators of specific subtypes of
  *    [[dbis.pig.op.PigOperator]] that are in a parent-child relationship into a strategy and adds it to this object.
  *
  *  - [[buildBinaryPigOperatorStrategy]] is the same as [[addBinaryPigOperatorStrategy]] but doesn't add the
  *    strategy to this object.
  *
  *  - [[processPlan]] applies a single strategy to a [[DataflowPlan]].
  *
  * ==Higher-level methods==
  *
  * For common operations on [[DataflowPlan]]s, some convenience methods to build and register a strategy are provided:
  *
  *  - [[addStrategy]] for easily adding any method with a signature of `Any => Option[PigOperator]` as a strategy
  *
  *  - [[addOperatorReplacementStrategy]] is similar to [[addStrategy]] but has some special behaviour that's useful
  *    when all the function passed to it does is replacing one operator with another one.
  *
  *  - [[merge]] for merging two operators
  *
  *  - [[reorder]] for reordering two operators
  *
  * ==DataflowPlan helper methods==
  *
  * Some operations provided by [[DataflowPlan]] objects are not implemented there, but on this object. These include
  *
  *  - [[insertAfter]] to insert an operator as a child operator of another
  *
  *  - [[remove]] to remove an operator
  *
  *  - [[replace]] to replace one operator with another
  *
  *  - [[swap]] to swap the positions of two operators
  *
  *  ==Helper methods for maintaining connectivity==
  *
  *  After a rewriting operation, the `inputs` and `outputs` attribute of operators other than the rewritten ones
  *  might be changed (for example to accommodate new or deleted operators). To help maintaining these relationships,
  *  the method [[fixInputsAndOutputs]] in several versions is provided. Their documentation include hints in which
  *  cases they apply.
  *
  * @todo Not all links in this documentation link to the correct methods, most notably links to overloaded ones.
  *
  */
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

  /** Adds a function `f` as a [[org.kiama.rewriting.Strategy]] to this object.
    *
    * If you're only intending to replace a single PigOperator with another one, use
    * [[addOperatorReplacementStrategy]] instead.
    *
    * @param f
    */
  //noinspection ScalaDocMissingParameterDescription
  def addStrategy(f: Any => Option[PigOperator]): Unit = addStrategy(strategyf(t => f(t)))

  /** Builds the strategy for [[addOperatorReplacementStrategy]].
    *
    * @param f
    * @return
    */
  def buildOperatorReplacementStrategy(f: Any => Option[PigOperator]): Strategy = strategyf(t =>
    f(t) map { op: PigOperator =>
      op.outputs foreach { output =>
        output.consumer foreach { consumer =>
          consumer.inputs foreach { input =>
            // If `t` (the old term) is the producer of any of the input pipes of `op` (the new terms) successors,
            // replace it with `op` in that attribute. Replacing `t` with `op` in the pipes on `op` itself is not
            // necessary because the setters of `inputs` and `outputs` do that.
            if (input.producer == t) {
              input.producer = op
            }
          }
        }
      }
      op
    }
  )

  /** Adds a function `f` that replaces a single [[PigOperator]] with another one as a [[org.kiama.rewriting.Strategy]]
    *  to this object.
    *
    * If applying `f` to a term succeeded (Some(_)) was returned, the input term will be replaced by the new term in
    * the input pipes of the new terms successors (the consumers of its output pipes).
    *
    * @param f
    */
  //noinspection ScalaDocMissingParameterDescription
  def addOperatorReplacementStrategy(f: Any => Option[PigOperator]): Unit = {
    addStrategy(buildOperatorReplacementStrategy(f))
  }

  /** Rewrites a given sink node with several [[org.kiama.rewriting.Strategy]]s that were added via
    * [[dbis.pig.plan.rewriting.Rewriter.addStrategy]].
    *
    * @param sink The sink node to rewrite.
    * @return The rewritten sink node.
    */
  def processPigOperator(sink: PigOperator): Any = {
    processPigOperator(sink, ourStrategy)
  }

  /** Process a sink with a specified strategy
    *
    * @param sink The sink to process.
    * @param strategy The strategy to apply.
    * @return
    */
  private def processPigOperator(sink: PigOperator, strategy: Strategy): Any = {
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
    val newSources = plan.sourceNodes.flatMap(
      processPigOperator(_, strategy) match {
        case op : PigOperator => List(op)
        case ops : Seq[PigOperator] => ops
        case e => throw new IllegalArgumentException("A rewriting operation returned something other than a " +
          "PigOperator or " +
          "Sequence of them, namely" + e)
      }).filterNot(_.isInstanceOf[Empty]).toList

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
    * An additional function `f` can be supplied that performs the reordering. This is useful if the reordering can
    * only be performed in some cases that can't be expressed by just the types.
    *
    * A new reordering strategy can be added to the rewriter via
    * {{{
    *  reorder[OrderBy, Filter](f _)
    * }}}
    *
    * @param f
    * @tparam T The type of the parent operator.
    * @tparam T2 The type of the child operator.
    */
  //noinspection ScalaDocMissingParameterDescription
  def reorder[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag](f: (T, T2) => Option[Tuple2[T2, T]]):
  Unit = {
    val strategy = (parent: T, child: T2) =>
      f(parent, child).map(tup => fixInputsAndOutputs(tup._2, tup._1, tup._1, tup._2))
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
  def reorder[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag]:
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

  /** Insert `newOp` between `in` and `out` in `plan`.
    *
    * @param plan The DataflowPlan in which to insert the new operator.
    * @param in The Operator being the input for the new operator.
    * @param out The Operator being the output for the new operator.
    * @param newOp The Operator getting inserted.
    * @return The new DataflowPlan.
    */
  def insertBetween(plan: DataflowPlan, inOp: PigOperator, outOp: PigOperator, newOp: PigOperator): DataflowPlan = {
    require(!newOp.isInstanceOf[Load], "Can't insert a Load operator after another operator")
    require(inOp.outputs.map(_.consumer).flatten.contains(outOp), "The input Operator must be directly connected to the output Operator")
    require(outOp.inputs.map(_.producer).contains(inOp), "The output Operator must be directly connected to the input Operator")
     
    //TODO:Outsource DISCONNECT, difficult since processPlan deletes Nodes with no output Pipe
    val outIdx = inOp.outputs.indexWhere(_.name == newOp.inputs.head.name)
    inOp.outputs(outIdx).consumer = inOp.outputs(outIdx).consumer.filterNot(_ == outOp)
    outOp.inputs = outOp.inputs.filterNot(_.producer == inOp)

        
    // Add new Operator to inOp consumers & its output Pipe to outOp inputs
    inOp.outputs(outIdx).consumer = inOp.outputs(outIdx).consumer :+ newOp
    outOp.inputs = outOp.inputs :+ newOp.outputs(outIdx)

    // Insert new Operator after inOp to Plan and append outOp
    var newPlan = plan.insertAfter(inOp,newOp)
    newPlan = newPlan.insertAfter(newOp,outOp)
    newPlan
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
        Some(repl)
      }
      else {
        None
      }
    }
    processPlan(plan, strategyf(t => strategy(t)))
  }

  /** Returns a strategy to remove `rem` from a DataflowPlan
   *
   * @param rem
   * @return
   */
  //noinspection ScalaDocMissingParameterDescription
  def removalStrategy(rem: PigOperator): Strategy = {
    strategyf((op: Any) => {
      if (op == rem) {
        val pigOp = op.asInstanceOf[PigOperator]
        if (pigOp.inputs.isEmpty) {
          val consumers = pigOp.outputs.flatMap(_.consumer)
          if (consumers.isEmpty) {
            Some(Empty(Pipe("")))
          }
          else {
            consumers foreach (_.inputs = List.empty)
            Some(consumers.toList)
          }
        }
        else {
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
      }
      else {
        None
      }
    })}

  /** Removes `rem` from `plan`.
    *
    * If `rem` has any child nodes in the plan, they will take its place.
    *
    * @param plan
    * @param rem
    * @param removePredecessors If true, predecessors of `rem` will be removed as well.
    * @return A new [[dbis.pig.plan.DataflowPlan]] without `rem`.
    */
  //noinspection ScalaDocMissingParameterDescription
  def remove(plan: DataflowPlan, rem: PigOperator, removePredecessors: Boolean = false): DataflowPlan = {
    var strat = removalStrategy(rem)

    if (removePredecessors) {
      var nodes = Set[PigOperator]()
      var nodesToProcess = rem.inputs.map(_.producer).toSet

      while (nodesToProcess.nonEmpty) {
        val iter = nodesToProcess.iterator
        nodesToProcess = Set[PigOperator]()
        for (node <- iter) {
          nodes += node
          nodesToProcess ++= node.inputs.map(_.producer).toSet
        }
      }

      strat = nodes.foldLeft(strat) ((s: Strategy, p: PigOperator) => ior(s, removalStrategy(p)))
    }

    processPlan(plan, strat)
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
  def addBinaryPigOperatorStrategy[T2 <: PigOperator : ClassTag, T <: PigOperator : ClassTag](f: (T, T2)
    => Option[PigOperator]): Unit = {
    val strategy = buildBinaryPigOperatorStrategy(f)
    addStrategy(strategy)
  }

  def buildBinaryPigOperatorStrategy[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag]
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
  def fixInputsAndOutputs[T <: PigOperator, T2 <: PigOperator, T3 <: PigOperator](oldParent: T, oldChild: T2,
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
  def fixInputsAndOutputs[T <: PigOperator, T2 <: PigOperator](oldParent: T, newParent: T2, oldChild: T2,
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
        val storer = new Store(materialize.inputs.head, file, "BinStorage")

        newPlan = plan.insertAfter(materialize.inputs.head.producer, storer)
        newPlan = newPlan.remove(materialize)

        logger.info(s"inserted new store operator $storer")
      }
    }

    newPlan
  }

  def processWindows(plan: DataflowPlan): DataflowPlan = {
    require(plan != null, "Plan must not be null")

    var newPlan = plan

    val walker1 = new BreadthFirstTopDownWalker
    val walker2 = new BreadthFirstBottomUpWalker

    // All Window Ops: Group,Filter,Distinct,Limit,OrderBy,Foreach
    // Two modes: Group,Filter,Limit,Foreach
    // Terminator: Foreach, Join
 
    logger.debug(s"Searching for Window Operators")
    walker1.walk(newPlan){ op => 
      op match {
        case o: Window => {
          logger.debug(s"Found Window Operator")
          newPlan = markForWindowMode(newPlan, o)
        }
        case _ =>
      }
    }
    
    // Find and process Window Joins 
    val joins = ListBuffer.empty[Join]
    walker2.walk(newPlan){ op => 
      op match {
        case o: Join => joins += o
        case _ =>
      }
    }
    newPlan = processWindowJoins(newPlan, joins.toList)

    newPlan
  }

  def markForWindowMode(plan: DataflowPlan, windowOp: Window): DataflowPlan = {
    var lastOp: PigOperator =  new Empty(Pipe("empty"))
    val littleWalker = Queue(windowOp.outputs.flatMap(_.consumer).toSeq: _*)
    while(!littleWalker.isEmpty){
      val operator = littleWalker.dequeue()
      operator match {
        case o: Filter => {
          logger.debug(s"Rewrite Filter to WindowMode")
          o.windowMode = true
        }
        case o: Limit => {
          logger.debug(s"Rewrite Limit to WindowMode")
          o.windowMode = true
        }
        case o: Distinct => {
          logger.debug(s"Rewrite Distinct to WindowMode")
          o.windowMode = true
        }
       case o: Grouping => {
          logger.debug(s"Rewrite Grouping to WindowMode")
          o.windowMode = true
        }
        case o: Foreach => {
          logger.debug(s"Rewrite Foreach to WindowMode")
          o.windowMode = true
          val flatten = new WindowFlatten(Pipe("flattenNode"), o.outputs.head)
          val newPlan = plan.insertBetween(o, o.outputs.head.consumer.head, flatten)
          return newPlan
        }
        case o: Join => {
          logger.debug(s"Found Join Node, abort")
          return plan
        }
        case _ =>
      }
      littleWalker ++= operator.outputs.flatMap(_.consumer)
      if (littleWalker.isEmpty) lastOp = operator
    }

    logger.debug(s"Reached End of Plan - Adding Flatten Node")
    val before = lastOp.inputs.head
    val flatten = new WindowFlatten(Pipe("flattenNode"), before.producer.outputs.head)
    val newPlan = plan.insertBetween(before.producer, lastOp, flatten)

    newPlan
  }

  def processWindowJoins(plan: DataflowPlan, joins: List[Join]): DataflowPlan = {

    var newPlan = plan
    
    /*
     * Foreach Join Operator check if Input requirements are met.
     * Collect Window input relations and create new Join with Window 
     * definition and window inputs as new inputs.
     */
    for(joinOp <- joins) {
      var newInputs = ListBuffer.empty[Pipe]
      var windowDef: Option[Tuple2[Int,String]] = None

      for(joinInputPipe <- joinOp.inputs){
        // Checks
        if(!joinInputPipe.producer.isInstanceOf[Window]) 
          throw new RewriterException("Join inputs must be Window Definitions")
        val inputWindow = joinInputPipe.producer.asInstanceOf[Window]
        if(inputWindow.window._2=="") 
          throw new RewriterException("Join input windows must be defined via RANGE")
        if (!windowDef.isDefined) 
          windowDef = Some(inputWindow.window)
        if(windowDef!=Some(inputWindow.window)) 
          throw new RewriterException("Join input windows must have the same definition")

        newInputs += inputWindow.in
        
        // Remove Window-Join relations
        joinOp.inputs = joinOp.inputs.filterNot(_.producer == inputWindow)
        inputWindow.outputs.foreach((out: Pipe) => {
          if(out.consumer contains joinOp)
            out.consumer = out.consumer.filterNot(_ == joinOp)
        })
      }

      val newJoin = Join(joinOp.out, newInputs.toList, joinOp.fieldExprs, 
                         windowDef.getOrElse(null.asInstanceOf[Tuple2[Int,String]]))
      /*
       * Replace Old Join with new Join (new Input Pipes and Window Parameter)
       */
      val strategy = (op: Any) => {
        if (op == joinOp) {
          joinOp.outputs = List.empty
          joinOp.inputs = List.empty
          Some(newJoin)
        }
        else {
          None
        }
      }
      newPlan = processPlan(newPlan, strategyf(t => strategy(t)))
    }
    newPlan
  }
  
  registerAllRules
}
