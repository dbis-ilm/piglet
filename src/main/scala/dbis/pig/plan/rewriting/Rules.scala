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

import dbis.pig.op._
import dbis.pig.plan.rewriting.Rewriter._
import org.kiama.rewriting.Rewriter._


/** This object contains all the rewriting rules that are currently implemented
  *
  */
object Rules {
  /** Put Filters before Joins if we can figure out which side of the join contains the fields used in the Filters
    * predicate
    *
    */
  def filterBeforeJoin(join: Join, filter: Filter): Option[Filter] = {
    // Collect all Fields of the Filter operator
    def extractFields(p: Expr): List[Ref] =
      p match {
        case RefExpr(Value(_)) => Nil
        case RefExpr(r) => List(r)
        case BinaryExpr(l, r) => extractFields(l) ++ extractFields(r)
        case And(l, r) => extractFields(l) ++ extractFields(r)
        case Or(l, r) => extractFields(l) ++ extractFields(r)
        case Not(p) => extractFields(p)
        case FlattenExpr(e) => extractFields(e)
        case PExpr(e) => extractFields(e)
        case CastExpr(t, e) => extractFields(e)
        case MSign(e) => extractFields(e)
        case Func(f, p) => p.flatMap(extractFields)
        case ConstructBagExpr(ex) => ex.flatMap(extractFields)
        case ConstructMapExpr(ex) => ex.flatMap(extractFields)
        case ConstructTupleExpr(ex) => ex.flatMap(extractFields)
      }

    val fields = extractFields(filter.pred)

    if (fields.exists(field => !field.isInstanceOf[NamedField])) {
      // A field is not a NamedField, we don't know how to figure out where it's coming from
      return None
    }

    val namedfields = fields.map(_.asInstanceOf[NamedField])

    val inputs = join.inputs.map(_.producer)

    var inputWithCorrectFields: Option[PigOperator] = None

    inputs foreach { inp =>
      namedfields foreach { f =>
        if (inp.schema.isEmpty) {
          // The schema of an input is not defined, abort because it might or might not contain one of the fields
          // we're looking for
          return None
        }

        if (inp.schema.get.fields.exists(_.name == f.name)) {
          if (inputWithCorrectFields.isEmpty) {
            // We found an input that has the correct field and we haven't found one with a matching field before
            inputWithCorrectFields = Some(inp)
          }

          if (inputWithCorrectFields.get != inp) {
            // We found an input that has a field we're looking for but we already found an input with a matching
            // field but it's not the same as the current input.
            // TODO Just abort the operation for now - in the future, we could try to split push different Filters into
            // multiple inputs of the Join.
            return None
          }
        }
      }
    }

    if (inputWithCorrectFields.isEmpty) {
      // We did not find an input that has all the fields we're looking for - this might be because the fields are
      // spread over multiple input or because the fields do not actually exist - in any case, abort.
      return None
    }

    // We found a single input that has all the fields used in the predicate, which means that we can safely put the
    // Filter between that and the Join.
    // We can't use fixInputsAndOutputs here because they don't work correctly for Joins
    val inp = inputWithCorrectFields.get

    // First, make the Filter a consumer of the correct input
    inp.outputs foreach { outp =>
      if (outp.consumer contains join) {
        outp.consumer = outp.consumer.filterNot(_ == join) :+ filter
      }
    }

    filter.inputs.filterNot(_.producer == join) :+ Pipe(inp.outPipeName, inp)

    // Second, make the Filter an input of the Join
    join.inputs = join.inputs.filterNot(_.name == inp.outPipeName) :+ Pipe(filter.outPipeName, filter)

    // Third, replace the Filter in the Joins outputs with the Filters outputs
    join.outputs foreach { outp =>
      if (outp.consumer contains filter) {
        outp.consumer = outp.consumer.filterNot(_ == filter) ++ filter.outputs.flatMap(_.consumer)
      }
    }

    // Fourth, make the Join the producer of all the Filters outputs inputs
    filter.outputs foreach { outp =>
      outp.consumer foreach { cons =>
        cons.inputs map { cinp =>
          if (cinp.producer == filter) {
            Pipe(join.outPipeName, join)
          } else {
            cinp
          }
        }
      }
    }
    Some(filter)
  }

  /** Merges two [[dbis.pig.op.Filter]] operations if one is the only input of the other.
    *
    * @param parent The parent filter.
    * @param child The child filter.
    * @return On success, an Option containing a new [[dbis.pig.op.Filter]] operator with the predicates of both input
    *         Filters, None otherwise.
    */
  def mergeFilters(parent: Filter, child: Filter): Option[PigOperator] =
    Some(Filter(child.out, parent.in, And(parent.pred, child.pred)))

  def splitIntoToFilters(node: Any): Option[List[PigOperator]] = node match {
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
  def removeNonStorageSinks(node: Any): Option[PigOperator] = node match {
    // Store and Dump are ok
    case Store(_, _, _, _) => None
    case Dump(_) => None
    // To prevent recursion, empty is ok as well
    case Empty(_) => None
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

  /** Applies rewriting rule R1 of the paper "SPARQling Pig - Processing Linked Data with Pig latin".
    *
    * @param term
    * @return Some Load operator, if `term` was an RDFLoad operator loading a remote resource
    */
  //noinspection ScalaDocMissingParameterDescription
  def R1(term: Any): Option[PigOperator] = term match {
    case op@RDFLoad(p, uri, None) =>
      if (uri.getScheme == "http" || uri.getScheme == "https") {
        Some(Load(p, uri, op.schema, "pig.SPARQLLoader", List("SELECT * WHERE { ?s ?p ?o }")))
      } else {
        None
      }
    case _ => None
  }

  /** Applies rewriting rule R2 of the paper "SPARQling Pig - Processing Linked Data with Pig latin".
    *
    * @param parent
    * @param child
    * @return
    */
  //noinspection ScalaDocMissingParameterDescription
  def R2(parent: RDFLoad, child: BGPFilter): Option[PigOperator] = {
    Some(Load(child.out, parent.uri, parent.schema, "pig.SPARQLLoader",
      List(child.patterns.head.toString))
    )
  }

  /** Applies rewriting rule L2 of the paper "SPARQling Pig - Processing Linked Data with Pig latin".
    *
    * @param term
    * @return Some Load operator, if `term` was an RDFLoad operator loading a resource from hdfs
    */
  //noinspection ScalaDocMissingParameterDescription
  def L2(term: Any): Option[PigOperator] = term match {
    case op@RDFLoad(p, uri, grouped : Some[String]) =>
      if (uri.getScheme == "hdfs") {
        Some(Load(p, uri, op.schema, "BinStorage"))
      } else {
        None
      }
    case _ => None
  }

  /** Applies rewriting rule F1 of the paper "SPARQling Pig - Processing Linked Data with Pig latin".
    *
    * @return A strategy that removes BGPFilters that use only unbound variables in their single pattern
    */
  def F1 = rulefs[BGPFilter] {
    case op@BGPFilter(_, _, patterns) =>
      if (patterns.length > 1 || patterns.isEmpty) {
        fail
      } else {
        val pattern = patterns.head
        if (!pattern.subj.isInstanceOf[Value]
          && !pattern.pred.isInstanceOf[Value]
          && !pattern.obj.isInstanceOf[Value]) {
          removalStrategy(op)
        } else {
          fail
        }
      }
    case _ => fail
  }

  /** Applies rewriting rule F2 of the paper "SPARQling Pig - Processing Linked Data with Pig latin".
    *
    * @param term
    * @return Some Filter operator, if `term` was an BGPFilter operator with only one bound variable
    */
  def F2(term: Any): Option[Filter] = term match {
    case op @ BGPFilter(in, out, patterns) =>
      if (patterns.length != 1) {
        return None
      }

      val pattern = patterns.head
      if (pattern.subj.isInstanceOf[Value]
        && !pattern.pred.isInstanceOf[Value]
        && !pattern.obj.isInstanceOf[Value]) {
        return Some(Filter(in, out, Eq(RefExpr(NamedField("subject")), RefExpr(pattern.subj))))
      } else if (!pattern.subj.isInstanceOf[Value]
        && pattern.pred.isInstanceOf[Value]
        && !pattern.obj.isInstanceOf[Value]) {
        return Some(Filter(in, out, Eq(RefExpr(NamedField("predicate")), RefExpr(pattern.pred))))
      } else if (!pattern.subj.isInstanceOf[Value]
        && !pattern.pred.isInstanceOf[Value]
        && pattern.obj.isInstanceOf[Value]) {
        return Some(Filter(in, out, Eq(RefExpr(NamedField("object")), RefExpr(pattern.obj))))
      }

      return None
    case _ => None
  }

  def registerAllRules = {
    merge[Filter, Filter](mergeFilters)
    merge[PigOperator, Empty](mergeWithEmpty)
    merge[RDFLoad, BGPFilter](R2)
    reorder[OrderBy, Filter]
    addStrategy(buildBinaryPigOperatorStrategy(filterBeforeJoin))
    addStrategy(strategyf(t => splitIntoToFilters(t)))
    addStrategy(removeNonStorageSinks _)
    addStrategy(R1 _)
    addStrategy(L2 _)
    addStrategy(F1)
    addStrategy(F2 _)
  }
}
