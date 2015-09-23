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

import dbis.pig.plan.rewriting.internals.{RDF, Column, PipeNameGenerator, FilterUtils}

import scala.collection.mutable.ListBuffer
import dbis.pig.op._
import Column.Column
import FilterUtils._
import dbis.pig.plan.rewriting.Rewriter._
import PipeNameGenerator.generate
import dbis.pig.schema.{Field, Types}
import org.kiama.rewriting.Rewriter._
import org.kiama.rewriting.Strategy


/** This object contains all the rewriting rules that are currently implemented
  *
  */
object Rules {
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
    Some(Filter(child.outputs.head, parent.inputs.head, And(parent.pred, child.pred)))
  }

  /** Removes a [[dbis.pig.op.Filter]] object that's a successor of a Filter with the same Predicate
    */
  def removeDuplicateFilters = rulefs[Filter] {
    case op@Filter(_, _, pred, _) => {
      topdown(
        attempt(
          op.outputs.flatMap(_.consumer).
            filter(_.isInstanceOf[Filter]).
            filter { f: PigOperator => extractPredicate(f.asInstanceOf[Filter].pred) == extractPredicate(pred) }.
            foldLeft(fail) { (s: Strategy, pigOp: PigOperator) => ior(s, buildRemovalStrategy(pigOp)
          )
          }))
    }
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

  /** Applies rewriting rule R1 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param term
    * @return Some Load operator, if `term` was an RDFLoad operator loading a remote resource
    */
  //noinspection ScalaDocMissingParameterDescription
  def R1(op: RDFLoad): Option[Load] = {
    // Only apply this rule if `op` is not followed by a BGPFilter operator. If it is, R2 applies.
    if (op.outputs.flatMap(_.consumer).exists(_.isInstanceOf[BGPFilter])) {
      return None
    }

    if (op.BGPFilterIsReachable) {
      return None
    }

    val uri = op.uri

    if (uri.getScheme == "http" || uri.getScheme == "https") {
      Some(Load(op.outputs.head, uri, op.schema, Some("pig.SPARQLLoader"), List("SELECT * WHERE { ?s ?p ?o }")))
    } else {
      None
    }
  }

  /** Applies rewriting rule R2 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    */
  //noinspection ScalaDocMissingParameterDescription
  def R2 = rulefs[RDFLoad] {
    case op =>

      /** Finds the next BGPFilter object reachable from ``op``.
        */
      def nextBGPFilter(op: PigOperator): Option[BGPFilter] = op match {
        case bf@BGPFilter(_, _, _) => Some(bf)
        // We need to make sure that each intermediate operator has only one successor - if it has multiple, we can't
        // pull up the BGPFilter because its patterns don't apply to all successors of the RDFLoad
        case _: OrderBy | _: Distinct | _: Limit | _: RDFLoad
          if op.outputs.flatMap(_.consumer).length == 1 => op.outputs.flatMap(_.consumer).map(nextBGPFilter).head
        case _ => None
      }

      val bf = nextBGPFilter(op)

      if (bf.isDefined) {
        // This is the function we'll use for replacing RDFLoad with Load
        def replacer = buildOperatorReplacementStrategy { sop: Any =>
          if (sop == op) {
            Some(Load(op.outputs.head, op.uri, op.schema, Some("pig.SPARQLLoader"),
              List("CONSTRUCT * WHERE " + RDF.triplePatternsToString(bf.get.patterns))))
          } else {
            None
          }
        }

        // This is the function we'll use to remove the BGPFilter
        def remover = topdown(attempt(buildRemovalStrategy(bf.get)))

        val strategy = ior(replacer, remover)
        strategy
      }
      else {
        fail
      }
  }

  /** Applies rewriting rule L2 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param term
    * @return Some Load operator, if `term` was an RDFLoad operator loading a resource from hdfs
    */
  //noinspection ScalaDocMissingParameterDescription
  def L2(op: RDFLoad): Option[Load] = {
    if (op.schema.isEmpty) {
      return None
    }

    Some(Load(op.out, op.uri, op.schema, Some("BinStorage")))
  }

  /** Applies rewriting rule F1 of the paper [[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @return A strategy that removes BGPFilters that use only unbound variables in their single pattern
    */
  def F1 = rulefs[BGPFilter] {
    case op@BGPFilter(_, _, patterns) =>
      if (patterns.length > 1 || patterns.isEmpty) {
        fail
      } else {
        val pattern = patterns.head
        if (RDF.allUnbound(pattern)) {
          buildRemovalStrategy(op)
        } else {
          fail
        }
      }
    case _ => fail
  }

  /** Applies rewriting rule F2 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param term
    * @return Some Filter operator, if `term` was an BGPFilter operator with only one bound variable
    */
  def F2(op: BGPFilter): Option[Filter] = {
    val in = op.inputs.head
    val out = op.outputs.head
    val patterns = op.patterns

    if (op.inputSchema != RDFLoad.plainSchema) {
      return None
    }

    if (patterns.length != 1) {
      return None
    }

    val pattern = patterns.head
    var filter: Option[Filter] = None
    val bound_column = RDF.getBoundColumn(pattern)

    filter = bound_column.flatMap { col: Column =>
      if (col == Column.Subject) {
        Some(Filter(out, in, Eq(RefExpr(NamedField("subject")), RefExpr(pattern.subj))))
      } else if (col == Column.Predicate) {
        Some(Filter(out, in, Eq(RefExpr(NamedField("predicate")), RefExpr(pattern.pred))))
      } else if (col == Column.Object) {
        Some(Filter(out, in, Eq(RefExpr(NamedField("object")), RefExpr(pattern.obj))))
      } else {
        // In reality, one of the above cases should always match
        None
      }
    }

    if (filter.isDefined) {
      in.removeConsumer(op)
    }

    return filter
  }

  /** Applies rewriting rule F3 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param term
    * @return Some Filter operator, if `term` was an BGPFilter operator with multiple bound variables
    */
  def F3(op: BGPFilter): Option[Filter] = {
    val patterns = op.patterns
    val in = op.inputs.head
    val out = op.outputs.head
    if (op.inputSchema != RDFLoad.plainSchema) {
      return None
    }

    if (patterns.length != 1) {
      return None
    }

    // We'll reuse in later on, so we need to remove `op` from its consumers
    in.removeConsumer(op)

    patterns.head match {
      case TriplePattern(s@Value(_), p@Value(_), o@Value(_)) => Some(
        Filter(out, in, And(
          Eq(RefExpr(NamedField("subject")), RefExpr(s)),
          And(
            Eq(RefExpr(NamedField("predicate")), RefExpr(p)),
            Eq(RefExpr(NamedField("object")), RefExpr(o))))))
      case TriplePattern(s@Value(_), p@Value(_), _) => Some(
        Filter(out, in, And(
          Eq(RefExpr(NamedField("subject")), RefExpr(Value("subject"))),
          Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicate"))))))
      case TriplePattern(s@Value(_), _, o@Value(_)) => Some(
        Filter(out, in, And(
          Eq(RefExpr(NamedField("subject")), RefExpr(Value("subject"))),
          Eq(RefExpr(NamedField("object")), RefExpr(Value("object"))))))
      case TriplePattern(_, p@Value(_), o@Value(_)) => Some(
        Filter(out, in, And(
          Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicate"))),
          Eq(RefExpr(NamedField("object")), RefExpr(Value("object"))))))
      case _ => None
    }
  }

  /** Applies rewriting rule F4 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param term
    * @return Some Filter operator if `term` was a BGPFilter with a single Pattern filtering on the grouping column
    *         of data in the triple group format
    */
  def F4(op: BGPFilter): Option[Filter] = {
    val patterns = op.patterns
    val in = op.inputs.head
    val out = op.outputs.head
    if (op.inputSchema == RDFLoad.plainSchema) {
      return None
    }

    if (op.inputSchema.isEmpty
      || !RDFLoad.groupedSchemas.values.toList.contains(op.inputSchema.get)) {
      return None
    }

    if (patterns.length != 1) {
      return None
    }
    // TODO we make a lot of assumptions about Options and Array lengths here
    val grouped_by = op.inputSchema.get.element.valueType.fields.head.name

    val pattern = patterns.head
    var filter: Option[Filter] = None
    val bound_column = RDF.getBoundColumn(pattern)

    filter = bound_column.flatMap { col: Column =>
      if (col == Column.Subject
        && grouped_by == "subject") {
        Some(Filter(out, in, Eq(RefExpr(NamedField("subject")), RefExpr(pattern.subj))))
      } else if (col == Column.Predicate
        && grouped_by == "predicate") {
        Some(Filter(out, in, Eq(RefExpr(NamedField("predicate")), RefExpr(pattern.pred))))
      } else if (col == Column.Object
        && grouped_by == "object") {
        Some(Filter(out, in, Eq(RefExpr(NamedField("object")), RefExpr(pattern.obj))))
      } else {
        // In reality, one of the above cases should always match
        None
      }
    }

    if (filter.isDefined) {
      in.removeConsumer(op)
    }

    return filter
  }

  def F5(term: Any): Option[Foreach] = term match {
    case op@BGPFilter(_, _, patterns) =>
      val in = op.inputs.head
      val out = op.outputs.head

      if (op.inputSchema == RDFLoad.plainSchema) {
        return None
      }

      if (op.inputSchema.isEmpty
        || !RDFLoad.groupedSchemas.values.toList.contains(op.inputSchema.get)) {
        return None
      }

      if (patterns.length != 1) {
        return None
      }

      // TODO we make a lot of assumptions about Options and Array lengths here
      val grouped_by = op.inputSchema.get.element.valueType.fields.head.name
      val pattern = patterns.head

      // Check if the column that's grouped by is not bound in this pattern
      val bound_column = RDF.getBoundColumn(pattern)

      val applies = grouped_by match {
        case "subject" if bound_column contains Column.Subject => false
        case "predicate" if bound_column contains Column.Predicate => false
        case "object" if bound_column contains Column.Object => false
        // Just in case there's no bound column
        case _ if bound_column.isEmpty => false
        case _ => true
      }

      // If not, this rule doesn't apply
      if (!applies) {
        return None
      }

      val internalPipeName = generate
      val intermediateResultName = generate
      val filter_by = Column.columnToNamedField(bound_column.get)
      val filter_value = bound_column.get match {
        case Column.Subject => pattern.subj
        case Column.Predicate => pattern.pred
        case Column.Object => pattern.obj
      }

      val foreach =
        Foreach(Pipe(internalPipeName), Pipe(in.name), GeneratorPlan(List(
          Filter(Pipe(intermediateResultName), Pipe("stmts"), Eq(RefExpr(filter_by), RefExpr(filter_value))),
          Generate(
            List(
              GeneratorExpr(RefExpr(NamedField("*"))),
              GeneratorExpr(Func("COUNT",
                List(RefExpr(NamedField(intermediateResultName)))),
                Some(Field("cnt", Types.ByteArrayType))))))))

      val filter = Filter(out, Pipe(internalPipeName, foreach),
        Gt(RefExpr(NamedField("cnt")), RefExpr(Value(0))))

      foreach.outputs foreach (_.addConsumer(filter))

      filter.outputs foreach { output =>
        output.consumer foreach { consumer =>
          consumer.inputs foreach { input =>
            // If `op` (the old term) is the producer of any of the input pipes of `filter` (the new terms)
            // successors, replace it with `filter` in that attribute. Replacing `op` with `other_filter` in
            // the pipes on `filter` itself is not necessary because the setters of `inputs` and `outputs` do
            // that.
            if (input.producer == op) {
              input.producer = filter
            }
          }
        }
      }

      in.removeConsumer(op)

      Some(foreach)
    case _ => None
  }

  /** Applies rewriting rule F7 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param term
    * @return Some BGPFilter operator if `term` was a BGPFilter with a single Pattern with two bound variables of which
    *         one is the grouping column
    */
  def F7(op: BGPFilter): Option[BGPFilter] = {
    val patterns = op.patterns
    val in = op.inputs.head
    val out = op.outputs.head
    if (op.inputSchema == RDFLoad.plainSchema) {
      return None
    }

    if (op.inputSchema.isEmpty
      || !RDFLoad.groupedSchemas.values.toList.contains(op.inputSchema.get)) {
      return None
    }

    if (patterns.length != 1) {
      return None
    }

    // TODO we make a lot of assumptions about Options and Array lengths here
    val grouped_by = op.inputSchema.get.element.valueType.fields.head.name

    val pattern = patterns.head

    // Check if the column that's grouped by is bound in this pattern
    val bound_columns = RDF.getAllBoundColumns(pattern)
    val applies = grouped_by match {
      case "subject" if bound_columns contains Column.Subject => true
      case "predicate" if bound_columns contains Column.Predicate => true
      case "object" if bound_columns contains Column.Object => true
      case _ => false
    }

    // If not, this rule doesn't apply
    if (!(applies && bound_columns.length == 2)) {
      return None
    }

    val internalPipeName = generate
    var group_filter: Option[BGPFilter] = None
    var other_filter_pattern: Option[TriplePattern] = None

    // The first pattern in the next 3 pattern matches is for the case where the column that the data is grouped by
    // is bound as a variable in the pattern, but the other two column are as well.
    if (grouped_by == "subject") {
      group_filter = Some(BGPFilter(Pipe(internalPipeName), in, List(TriplePattern(pattern.subj, PositionalField(1)
        , PositionalField(2)))))
      other_filter_pattern = pattern match {
        case TriplePattern(_, Value(_), Value(_)) => None
        case TriplePattern(_, pred@Value(_), _) => Some(TriplePattern(PositionalField(0), pred, PositionalField(2)))
        case TriplePattern(_, _, obj@Value(_)) => Some(TriplePattern(PositionalField(0), PositionalField(1), obj))
      }
    } else if (grouped_by == "predicate") {
      group_filter = Some(BGPFilter(Pipe(internalPipeName), in, List(TriplePattern(PositionalField(0), pattern
        .pred, PositionalField(2)))))
      other_filter_pattern = pattern match {
        case TriplePattern(Value(_), _, Value(_)) => None
        case TriplePattern(subj@Value(_), _, _) => Some(TriplePattern(subj, PositionalField(1), PositionalField
          (2)))
        case TriplePattern(_, _, obj@Value(_)) => Some(TriplePattern(PositionalField(0), PositionalField(1), obj))
      }
    } else if (grouped_by == "object") {
      group_filter = Some(BGPFilter(Pipe(internalPipeName), in, List(TriplePattern(PositionalField(0),
        PositionalField(1), pattern.obj))))
      other_filter_pattern = pattern match {
        case TriplePattern(Value(_), Value(_), _) => None
        case TriplePattern(subj@Value(_), _, _) => Some(TriplePattern(subj, PositionalField(1), PositionalField
          (2)))
        case TriplePattern(_, pred@Value(_), _) => Some(TriplePattern(PositionalField(0), pred, PositionalField(2)))
      }
    }

    if (other_filter_pattern.isEmpty) {
      // The grouping column is bound and the other two are as well - this rule doesn't apply.
      return None
    }

    val other_filter = BGPFilter(out, Pipe(internalPipeName, group_filter.get), List(other_filter_pattern.get))

    other_filter.outputs foreach { output =>
      output.consumer foreach { consumer =>
        consumer.inputs foreach { input =>
          // If `op` (the old term) is the producer of any of the input pipes of `other_filter` (the new terms)
          // successors, replace it with `other_filter` in that attribute. Replacing `op` with `other_filter` in
          // the pipes on `other_filter` itself is not necessary because the setters of `inputs` and `outputs` do
          // that.
          if (input.producer == op) {
            input.producer = other_filter
          }
        }
      }
    }

    group_filter foreach {
      _.outputs.head.consumer = List(other_filter)
    }

    if (group_filter.isDefined) {
      in.removeConsumer(op)
    }

    group_filter
  }

  /** Applies rewriting rule F8 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param term
    * @return Some BGPFilter operator if `term` was a BGPFilter with a single Pattern with only bound variables.
    */
  def F8(op: BGPFilter): Option[BGPFilter] = {
    val patterns = op.patterns
    val in = op.inputs.head
    val out = op.outputs.head
    if (op.inputSchema == RDFLoad.plainSchema) {
      return None
    }

    if (op.inputSchema.isEmpty
      || !RDFLoad.groupedSchemas.values.toList.contains(op.inputSchema.get)) {
      return None
    }

    if (patterns.length != 1) {
      return None
    }

    // TODO we make a lot of assumptions about Options and Array lengths here
    val grouped_by = op.inputSchema.get.element.valueType.fields.head.name

    val pattern = patterns.head

    if (!pattern.subj.isInstanceOf[Value]
      || !pattern.pred.isInstanceOf[Value]
      || !pattern.obj.isInstanceOf[Value]) {
      // The rule only applies if all variables are bound
      return None
    }

    val internalPipeName = generate
    var group_filter: Option[BGPFilter] = None
    var other_filter: Option[BGPFilter] = None

    if (grouped_by == "subject") {
      group_filter = Some(BGPFilter(Pipe(internalPipeName), in, List(TriplePattern(pattern.subj, PositionalField(1),
        PositionalField(2)))))
      other_filter = Some(BGPFilter(out, Pipe(internalPipeName), List(TriplePattern(PositionalField(0), pattern.pred,
        pattern.obj))))
    } else if (grouped_by == "predicate") {
      group_filter = Some(BGPFilter(Pipe(internalPipeName), in, List(TriplePattern(PositionalField(0), pattern.pred,
        PositionalField(2)))))
      other_filter = Some(BGPFilter(out, Pipe(internalPipeName), List(TriplePattern(pattern.subj, PositionalField(1),
        pattern.obj))))
    } else if (grouped_by == "object") {
      group_filter = Some(BGPFilter(Pipe(internalPipeName), in, List(TriplePattern(PositionalField(0),
        PositionalField(1),
        pattern.obj))))
      other_filter = Some(BGPFilter(out, Pipe(internalPipeName), List(TriplePattern(pattern.subj, pattern.pred,
        PositionalField(2)))))
    }

    other_filter foreach {
      _.outputs foreach { output =>
        output.consumer foreach { consumer =>
          consumer.inputs foreach { input =>
            // If `op` (the old term) is the producer of any of the input pipes of `other_filter` (the new terms)
            // successors, replace it with `other_filter` in that attribute. Replacing `op` with `other_filter` in
            // the pipes on `other_filter` itself is not necessary because the setters of `inputs` and `outputs` do
            // that.
            if (input.producer == op) {
              input.producer = other_filter.get
            }
          }
        }
      }
    }

    group_filter foreach {
      _.outputs.head.consumer = List(other_filter.get)
    }

    if (group_filter.isDefined) {
      in.removeConsumer(op)
    }

    group_filter
  }

  /** Applies rewriting rule F8 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param term
    * @return Some BGPFilter objects if the input filters BGP is a star join.
    */
  def J1(term: Any): Option[List[BGPFilter]] = term match {
    case op@BGPFilter(_, _, patterns) =>
      val out = op.outputs.head
      val in = op.inputs.head
      if (op.inputSchema != RDFLoad.plainSchema) {
        return None
      }

      if (patterns.length < 2) {
        return None
      }

      def isNamed(r: Ref): Option[NamedField] = r match {
        case n@NamedField(_) => Some(n)
        case _ => None
      }

      val namedFields = patterns map { p =>
        (isNamed(p.subj), isNamed(p.pred), isNamed(p.obj))
      } toSet

      if (namedFields.size != 1) {
        // There are either no NamedFields or they appear in more than one position in different patterns, so it's
        // not a star join
        return None
      }

      if (!RDF.isStarJoin(patterns)) {
        return None
      }

      // We'll reuse in later on, so we need to remove `op` from its consumers
      in.removeConsumer(op)

      val fieldname = RDF.starJoinColumn(patterns).get._2
      val filters = patterns map { p => BGPFilter(Pipe(generate), in, List(p)) }
      val join = Join(out,
        filters map { f => Pipe(f.outPipeName, f) },
        // Use map here to make sure the amount of field expressions is the same as the amount of filters
        filters map { _ => List(fieldname) })

      filters foreach { f => f.outputs.head.consumer = List(join) }

      return Some(filters)

    case _ => None
  }

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
            if (op.inputSchema.isEmpty)
              throw RewriterException("Rewriting * in GENERATE requires a schema")
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
        case GeneratorList(exprs) => {
          val (genExprs, foundStar) = constructGeneratorList(exprs, op)
          if (foundStar) {
            val newGen = GeneratorList(genExprs.toList)
            val newOp = Foreach(op.outputs.head, op.inputs.head, newGen, op.windowMode)
            newOp.constructSchema
            return Some(newOp)
          }
          else
            return None
        }
        case _ => None
      }
      case op@Generate(exprs) => {
        val (genExprs, foundStar) = constructGeneratorList(exprs, op)
        if (foundStar) {
          val newOp = Generate(genExprs.toList)
          newOp.copyPipes(op)
          newOp.constructSchema
          return Some(newOp)
        }
        else
          return None
      }
      case _ => None
    }
  }

  def registerAllRules = {
    addStrategy(removeDuplicateFilters)
    merge[Filter, Filter](mergeFilters)
    merge[PigOperator, Empty](mergeWithEmpty)
    reorder[OrderBy, Filter]
    addStrategy(buildBinaryPigOperatorStrategy[Join, Filter](filterBeforeMultipleInputOp))
    addStrategy(buildBinaryPigOperatorStrategy[Cross, Filter](filterBeforeMultipleInputOp))
    addStrategy(strategyf(t => splitIntoToFilters(t)))
    addStrategy(removeNonStorageSinks _)
    addOperatorReplacementStrategy(buildTypedCaseWrapper(R1))
    addStrategy(R2)
    addOperatorReplacementStrategy(buildTypedCaseWrapper(L2))
    addStrategy(F1)
    addOperatorReplacementStrategy(buildTypedCaseWrapper(F2))
    addOperatorReplacementStrategy(buildTypedCaseWrapper(F3))
    addOperatorReplacementStrategy(buildTypedCaseWrapper(F4))
    addTypedStrategy(F7)
    addTypedStrategy(F8)
    addStrategy(strategyf(t => J1(t)))
    addOperatorReplacementStrategy(foreachGenerateWithAsterisk _)
  }
}
