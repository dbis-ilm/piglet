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

import dbis.pig.tools.logging.PigletLogging
import dbis.pig.op.{PigOperator, _}
import dbis.pig.plan.DataflowPlan
import dbis.pig.plan.rewriting.Rules.registerAllRules
import dbis.pig.plan.rewriting.rulesets.GeneralRuleset._
import dbis.pig.plan.rewriting.dsl.RewriterDSL
import dbis.pig.plan.rewriting.internals._
import org.kiama.rewriting.Rewriter._
import org.kiama.rewriting.Rewriter.{rewrite => kiamarewrite}
import org.kiama.rewriting.Strategy
import scala.collection.mutable
import scala.reflect.ClassTag

case class RewriterException(msg: String) extends Exception(msg)

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
  * - [[addStrategy]] adds a single strategy to this object. Later calls to [[processPlan]] will then use this
  * strategy in addition to all other ones added via this method.
  *
  * - [[addBinaryPigOperatorStrategy]] turns a function operating on two operators of specific subtypes of
  * [[dbis.pig.op.PigOperator]] that are in a parent-child relationship into a strategy and adds it to this object.
  *
  * - [[buildBinaryPigOperatorStrategy]] is the same as [[addBinaryPigOperatorStrategy]] but doesn't add the
  * strategy to this object.
  *
  * - [[buildRemovalStrategy]] builds a strategy to remove an operator from a [[DataflowPlan]]
  *
  * - [[processPlan]] applies a single strategy to a [[DataflowPlan]].
  *
  * ==Higher-level methods==
  *
  * For common operations on [[DataflowPlan]]s, some convenience methods to build and register a strategy are provided:
  *
  * - [[addStrategy]] for easily adding any method with a signature of `Any => Option[PigOperator]` as a strategy
  *
  * - [[addOperatorReplacementStrategy]] is similar to [[addStrategy]] but has some special behaviour that's useful
  * when all the function passed to it does is replacing one operator with another one.
  *
  * - [[merge]] for merging two operators
  *
  * - [[reorder]] for reordering two operators
  *
  * ==DSL==
  *
  * A DSL for easily adding rewriting operations is provided as well, its documented in [[dbis.pig.plan.rewriting.dsl]].
  *
  * ==DataflowPlan helper methods==
  *
  * Some operations provided by [[DataflowPlan]] objects are not implemented there, but on this object. These include
  *
  * - [[insertAfter]] to insert an operator as a child operator of another
  *
  * - [[remove]] to remove an operator
  *
  * - [[replace]] to replace one operator with another
  *
  * - [[swap]] to swap the positions of two operators
  *
  * ==Helper methods for maintaining connectivity==
  *
  * After a rewriting operation, the `inputs` and `outputs` attribute of operators other than the rewritten ones
  * might be changed (for example to accommodate new or deleted operators). To help maintaining these relationships,
  * the methods [[fixMerge]], [[fixReordering]] and [[pullOpAcrossMultipleInputOp]] in several versions is provided.
  * Their documentation include hints in which cases they apply.
  *
  * @todo Not all links in this documentation link to the correct methods, most notably links to overloaded ones.
  *
  */
object Rewriter extends PigletLogging
                with StrategyBuilders
                with DFPSupport
                with WindowSupport
                with EmbedSupport
                with MaterializationSupport
                with Fixers
                with FastStrategyAdder
                with RewriterDSL {
  private var ourStrategy = fail

  /** Resets [[ourStrategy]] to [[fail]].
    *
    */
  private def resetStrategy = ourStrategy = fail

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

  /** Rewrites a given sink node with several [[org.kiama.rewriting.Strategy]]s that were added via
    * [[dbis.pig.plan.rewriting.Rewriter.addStrategy]].
    *
    * @param op The sink node to rewrite.
    * @return The rewritten sink node.
    */
  def processPigOperator(op: PigOperator): Any = {
    processPigOperator(op, ourStrategy)
  }

  /** Process a sink with a specified strategy
    *
    * @param op The sink to process.
    * @param strategy The strategy to apply.
    * @return
    */
  private def processPigOperator(op: PigOperator, strategy: Strategy): Any = {
    // TODO: We apply foreachRecursively separately because it always succeeds,
    // so we'd otherwise run into an infinite loop
    val newop = kiamarewrite(strategy)(op)
    newop
  }

  /** Apply all rewriting rules of this Rewriter to a [[dbis.pig.plan.DataflowPlan]].
    *
    * @param plan The plan to process.
    * @return A rewritten [[dbis.pig.plan.DataflowPlan]]
    */
  def processPlan(plan: DataflowPlan): DataflowPlan = {
    evalExtraRuleCode(plan.extraRuleCode)
    val forewriter = buildTypedCaseWrapper(foreachRecursively)
    val fostrat = manybu(strategyf(t => forewriter(t)))
    val rewriter = ior(outermost(ourStrategy), fostrat)
    processPlan(plan, rewriter)
  }
  def processPlan(plan: DataflowPlan, strategy: Strategy): DataflowPlan = {
    evalExtraRuleCode(plan.extraRuleCode)

    // This looks innocent, but this is where the rewriting happens.
    val newSources = plan.sourceNodes.flatMap(
      processPigOperator(_, strategy) match {
        case Nil => List.empty
        case op: PigOperator => List(op)
        case ops: Seq[PigOperator] => ops
        case e => throw new IllegalArgumentException(s"A rewriting operation returned something other than a " +
          "PigOperator or Sequence of them, namely $e")
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

    val newPlan = new DataflowPlan(newPlanNodes.toList.filterNot(_.isInstanceOf[Empty]))
    newPlan.additionalJars ++= plan.additionalJars
    newPlan.udfAliases ++= plan.udfAliases
    newPlan.code = plan.code
    newPlan
  }

  registerAllRules
}
