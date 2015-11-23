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
package dbis.pig.plan.rewriting.rulesets

import dbis.pig.op._
import dbis.pig.plan.{DataflowPlan, InvalidPlanException}
import dbis.pig.plan.rewriting.Rewriter._
import dbis.pig.plan.rewriting.{Rewriter, RewriterException}
import dbis.pig.plan.rewriting.internals.FilterUtils._
import org.kiama.rewriting.Rewriter._
import org.kiama.rewriting.Strategy
import scala.collection.mutable.ListBuffer
import dbis.pig.expr.NamedField
import dbis.pig.expr.And
import dbis.pig.expr.RefExpr

object GeneralRuleset extends Ruleset {
  /** Put Filters before multipleInputOp if we can figure out which input of multipleInputOp contains the fields used in the Filters predicate
    */
  def filterBeforeMultipleInputOp(multipleInputOp: PigOperator, filter: Filter): Option[Filter] = {
    val fields = extractFields(filter.pred)

    if (fields.exists(field => !field.isInstanceOf[NamedField])) {
      // A field is not a NamedField, we don't know how to figure out where it's coming from
      return None
    }

    val namedfields = fields.map(_.asInstanceOf[NamedField])
    val inputs = multipleInputOp.inputs.map(_.producer)
    var inputWithCorrectFields: Option[PigOperator] = findInputForFields(inputs, namedfields)

    if (inputWithCorrectFields.isEmpty) {
      // We did not find an input that has all the fields we're looking for - this might be because the fields are
      // spread over multiple input or because the fields do not actually exist - in any case, abort.
      return None
    }

    // We found a single input that has all the fields used in the predicate, which means that we can safely put the
    // Filter between that and the Join.
    // We can't use fixInputsAndOutputs here because they don't work correctly for Joins
    val inp = inputWithCorrectFields.get

    Some(pullOpAcrossMultipleInputOp(filter, multipleInputOp, inp).asInstanceOf[Filter])
  }

  /** Merges two [[dbis.pig.op.Filter]] operations if one is the only input of the other.
    *
    * @param parent The parent filter.
    * @param child The child filter.
    * @return On success, an Option containing a new [[dbis.pig.op.Filter]] operator with the predicates of both input
    *         Filters, None otherwise.
    */
  def mergeFilters(parent: Filter, child: Filter): Option[Filter] = {
    if (parent.pred != child.pred) {
      Some(Filter(child.outputs.head, parent.inputs.head, And(parent.pred, child.pred)))
    } else {
      None
    }
  }

  /** Removes a [[dbis.pig.op.Filter]] object that's a successor of a Filter with the same Predicate
    */
  //noinspection MutatorLikeMethodIsParameterless
  def removeDuplicateFilters = rulefs[Filter] {
    case op@Filter(_, _, pred, _) =>
      topdown(
        attempt(
          op.outputs.flatMap(_.consumer).
            filter(_.isInstanceOf[Filter]).
            filter { f: PigOperator => extractPredicate(f.asInstanceOf[Filter].pred) == extractPredicate(pred) }.
            foldLeft(fail) { (s: Strategy, pigOp: PigOperator) => ior(s, buildRemovalStrategy(pigOp)
          )
          }))
  }

  def splitIntoToFilters(node: Any): Option[List[Filter]] = node match {
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
            output.inputs = output.inputs.
              // First, remove the SplitInto operator
              filter(_.producer != node).
              // Second, remove the filter if it has already been added to this input once. This can happen if a Join
              // reads from multiple branches of a single SplitInto operator because of the nested loop nature of
              // this code because we then iterate over the Joins inputs more than once (the Join is the consumer of
              // multiple node.outputs).
              filter(_.producer != filters(input.name)) :+
              Pipe(input.name, filters(input .name))
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
  def removeNonStorageSinks(node: Any): Option[PigOperator] = node match {
    // Store and Dump are ok
    case Store(_, _, _, _) => None
    case Dump(_) => None
    // To prevent recursion, empty is ok as well
    case Empty(_) => None
    case Generate(_) => None
    case op: PigOperator =>
      op.outputs match {
        case Pipe(_, _, Nil) :: Nil | Nil =>
          val newNode = Empty(Pipe(""))
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
  def mergeWithEmpty(parent: PigOperator, child: Empty): Option[PigOperator] = Some(child)

  /**
   * Process the list of generator expressions in GENERATE and replace the * by the list of named fields
   *
   * @param exprs
   * @param op
   * @return
   */
  def constructGeneratorList(exprs: List[GeneratorExpr], op: PigOperator): (List[GeneratorExpr], Boolean) = {
    val genExprs: ListBuffer[GeneratorExpr] = ListBuffer()
    var foundStar: Boolean = false
    for (ex <- exprs) {
      if (ex.expr.isInstanceOf[RefExpr]) {
        val ref = ex.expr.asInstanceOf[RefExpr]
        if (ref.r.isInstanceOf[NamedField]) {
          val field = ref.r.asInstanceOf[NamedField]
          if (field.name == "*") {
            if (op.inputSchema.isEmpty) {
              throw RewriterException("Rewriting * in GENERATE requires a schema")
            }
            foundStar = true
            genExprs ++= op.inputSchema.get.fields.map(f => GeneratorExpr(RefExpr(NamedField(f.name))))
          }
          else genExprs += ex
        }
        else genExprs += ex
      }
      else genExprs += ex
    }
    (genExprs.toList, foundStar)
  }

  def foreachGenerateWithAsterisk(term: Any): Option[PigOperator] = {
    term match {
      case op@Foreach(_, _, gen, _) => gen match {
        case GeneratorList(exprs) =>
          val (genExprs, foundStar) = constructGeneratorList(exprs, op)
          if (foundStar) {
            val newGen = GeneratorList(genExprs.toList)
            val newOp = Foreach(op.outputs.head, op.inputs.head, newGen, op.windowMode)
            newOp.constructSchema
            Some(newOp)
          }
          else
            None
        case _ => None
      }
      case op@Generate(exprs) =>
        val (genExprs, foundStar) = constructGeneratorList(exprs, op.parentOp)
        if (foundStar) {
          val newOp = Generate(genExprs.toList)
          newOp.copyPipes(op)
          newOp.constructSchema
          Some(newOp)
        }
        else
          None
      case _ => None
    }
  }

  def foreachRecursively(fo: Foreach): Option[Foreach] = {
    fo.subPlan = fo.subPlan map {d: DataflowPlan => Rewriter.processPlan(d)}
    if (fo.subPlan.isDefined) {
      fo.generator = new GeneratorPlan(fo.subPlan.get.operators)
      Some(fo)
    }
    else
      None
  }

  def replaceMacroOp(t: Any): Option[PigOperator] = t match {
    case op@MacroOp(out, name, params) => {
      if (op.macroDefinition.isEmpty)
        throw new InvalidPlanException(s"macro ${op.macroName} undefined")

      val macroDef = op.macroDefinition.get
      val subPlan = macroDef.subPlan.get

      /*
       * adjust the parameter names
       */
      subPlan.resolveParameters(op.paramMapping)

      /*
       * and replace the macro call by its definition
       */
      val newParent = subPlan.operators.head
      val newChild = subPlan.operators.last
      val newOp = fixReplacementwithMultipleOperators(op, newParent, newChild)
      val schema = newOp.constructSchema
      Some(newOp)
    }
    case _ =>  None
  }

  def registerRules() = {
    // IMPORTANT: If you change one of the rule registration calls in here, please also change the call in the
    // corresponding test methods!
    addStrategy(replaceMacroOp _)
    addStrategy(removeDuplicateFilters)
    merge(mergeFilters)
    merge(mergeWithEmpty)
    reorder[OrderBy, Filter]
    addStrategy(buildBinaryPigOperatorStrategy[Join, Filter](filterBeforeMultipleInputOp))
    addStrategy(buildBinaryPigOperatorStrategy[Cross, Filter](filterBeforeMultipleInputOp))
    addStrategy(strategyf(t => splitIntoToFilters(t)))
    applyRule(foreachRecursively _)
    addStrategy(removeNonStorageSinks _)
    addOperatorReplacementStrategy(foreachGenerateWithAsterisk)
  }
}
