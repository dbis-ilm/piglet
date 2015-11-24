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
import dbis.pig.expr._
import dbis.pig.plan.PipeNameGenerator._
import dbis.pig.plan.rewriting.{Functions, Rewriter}
import dbis.pig.plan.rewriting.Rewriter._
import dbis.pig.plan.rewriting.internals.Column.Column
import dbis.pig.plan.rewriting.internals.{Column, RDF}
import dbis.pig.schema.{Field, Schema, Types}
import org.kiama.rewriting.Rewriter._
import scala.collection.mutable.Map

object RDFRuleset extends Ruleset {

  /** Finds the next BGPFilter object reachable from ``op``.
    */
  private def nextBGPFilter(op: PigOperator): Option[BGPFilter] = op match {
    case bf@BGPFilter(_, _, _) => Some(bf)
    // We need to make sure that each intermediate operator has only one successor - if it has multiple, we can't
    // pull up the BGPFilter because its patterns don't apply to all successors of the RDFLoad
    case _: OrderBy | _: Distinct | _: Limit | _: RDFLoad
      if op.outputs.flatMap(_.consumer).length == 1 => op.outputs.flatMap(_.consumer).map(nextBGPFilter).head
    case _ => None
  }

  /** Applies rewriting rule R1 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param op
    * @return Some Load operator, if `term` was an RDFLoad operator loading a remote resource
    */
  //noinspection ScalaDocMissingParameterDescription
  def R1(op: RDFLoad): Option[Load] = {
    // Only apply this rule if `op` is not followed by a BGPFilter operator. If it is, R2 applies.
    val next_bgpfilter = nextBGPFilter(op)
    if (next_bgpfilter.isDefined && next_bgpfilter.get.schema == op.schema) {
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


      val bf = nextBGPFilter(op)

      if (bf.isDefined && bf.get.schema == op.schema) {
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
    * @param op
    * @return Some Load operator, if `term` was an RDFLoad operator loading a resource from hdfs
    */
  //noinspection ScalaDocMissingParameterDescription
  def L2(op: RDFLoad): Option[Load] = {
    if (op.schema.isEmpty) {
      return None
    }

    Some(Load(op.out, op.uri, op.schema, Some("RDFFileStorage")))
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
    * @param op
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

    filter
  }

  /** Applies rewriting rule F3 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param op
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
          Eq(RefExpr(NamedField("subject")), RefExpr(s)),
          Eq(RefExpr(NamedField("predicate")), RefExpr(p)))))
      case TriplePattern(s@Value(_), _, o@Value(_)) => Some(
        Filter(out, in, And(
          Eq(RefExpr(NamedField("subject")), RefExpr(s)),
          Eq(RefExpr(NamedField("object")), RefExpr(o)))))
      case TriplePattern(_, p@Value(_), o@Value(_)) => Some(
        Filter(out, in, And(
          Eq(RefExpr(NamedField("predicate")), RefExpr(p)),
          Eq(RefExpr(NamedField("object")), RefExpr(o)))))
      case _ => None
    }
  }

  /** True, if `schema` is one of the grouped schemas, false otherwise.
    *
    * @param schema
    * @return
    */
  def groupedSchemaEarlyAbort(schema: Option[Schema]): Boolean = {
    if (schema == RDFLoad.plainSchema) {
      return true
    }

    if (schema.isEmpty
      || !RDFLoad.groupedSchemas.values.toList.contains(schema.get)) {
      return true
    }
    false
  }

  def groupedSchemaJoinEarlyAbort(op: BGPFilter): Boolean = {
    if (groupedSchemaEarlyAbort(op.inputSchema)) {
      return true
    }

    if (op.patterns.length < 2) {
      return true
    }

    false
  }

  def plainSchemaJoinEarlyAbort(op: BGPFilter): Boolean = {
    if (op.inputSchema != RDFLoad.plainSchema) {
      return true
    }

    if (op.patterns.length < 2) {
      return true
    }

    false
  }

  /** Applies rewriting rule F4 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param op
    * @return Some Filter operator if `term` was a BGPFilter with a single Pattern filtering on the grouping column
    *         of data in the triple group format
    */
  def F4(op: BGPFilter): Option[Filter] = {
    val patterns = op.patterns
    val in = op.inputs.head
    val out = op.outputs.head

    if (groupedSchemaEarlyAbort(op.inputSchema)) {
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

    filter
  }

  /** Applies rewriting rule F5 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param op
    * @return
    */
  def F5(op: BGPFilter): Option[(Foreach, Filter)] = {
    val in = op.inputs.head
    val out = op.outputs.head
    val patterns = op.patterns

    if (groupedSchemaEarlyAbort(op.inputSchema)) {
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

    val internalPipeName = generate()
    val intermediateResultName = generate()
    val eq = RDF.patternToConstraint(pattern).get

    val foreach =
      Foreach(Pipe(internalPipeName), Pipe(in.name), GeneratorPlan(List(
        Filter(Pipe(intermediateResultName), Pipe("stmts"), eq),
        Generate(
          List(
            GeneratorExpr(RefExpr(NamedField("*"))),
            GeneratorExpr(Func("COUNT",
              List(RefExpr(NamedField(intermediateResultName)))),
              Some(Field("cnt", Types.ByteArrayType))))))))

    val filter = Filter(out, Pipe(internalPipeName, foreach),
      Gt(RefExpr(NamedField("cnt")), RefExpr(Value(0))))

    Rewriter.connect(foreach, filter)

    Some((foreach, filter))
  }

  /** Applies rewriting rule F6 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param term
    * @return
    */
  def F6(op: BGPFilter): Option[(Foreach, Filter)] = {
    val in = op.inputs.head
    val out = op.outputs.head
    val patterns = op.patterns

    if (groupedSchemaEarlyAbort(op.inputSchema)) {
      return None
    }

    if (patterns.length != 1) {
      return None
    }

    // TODO we make a lot of assumptions about Options and Array lengths here
    val grouped_by = op.inputSchema.get.element.valueType.fields.head.name
    val pattern = patterns.head

    // Check if the column that's grouped by is not bound in this pattern
    val bound_columns = RDF.getAllBoundColumns(pattern)

    // If the number of bound variables in the pattern isn't 2, this rule doesn't apply.
    if (bound_columns.length != 2) {
      return None
    }

    val applies = grouped_by match {
      case "subject" if bound_columns contains Column.Subject => false
      case "predicate" if bound_columns contains Column.Predicate => false
      case "object" if bound_columns contains Column.Object => false
      // Just in case there's no bound column
      case _ if bound_columns.isEmpty => false
      case _ => true
    }

    // If not, this rule doesn't apply
    if (!applies) {
      return None
    }

    val internalPipeName = generate()
    val intermediateResultName = generate()
    val constraint = RDF.patternToConstraint(pattern).get

    val foreach =
      Foreach(Pipe(internalPipeName), Pipe(in.name), GeneratorPlan(List(
        Filter(Pipe(intermediateResultName), Pipe("stmts"), constraint),
        Generate(
          List(
            GeneratorExpr(RefExpr(NamedField("*"))),
            GeneratorExpr(Func("COUNT",
              List(RefExpr(NamedField(intermediateResultName)))),
              Some(Field("cnt", Types.ByteArrayType))))))))

    val filter = Filter(out, Pipe(internalPipeName, foreach),
      Gt(RefExpr(NamedField("cnt")), RefExpr(Value(0))))

    Rewriter.connect(foreach, filter)
    Rewriter.fixReplacementwithMultipleOperators(op, foreach, filter)

    Some((foreach, filter))
  }

  /** Applies rewriting rule F7 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param op
    * @return Some BGPFilter operator if `term` was a BGPFilter with a single Pattern with two bound variables of which
    *         one is the grouping column
    */
  def F7(op: BGPFilter): Option[BGPFilter] = {
    val patterns = op.patterns
    val in = op.inputs.head
    val out = op.outputs.head

    if (groupedSchemaEarlyAbort(op.inputSchema)) {
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

    val internalPipeName = generate()
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

    Rewriter.fixReplacementwithMultipleOperators(op, group_filter.get, other_filter)

    group_filter foreach {
      Rewriter.connect(_, other_filter)
    }


    if (group_filter.isDefined) {
      in.removeConsumer(op)
    }

    group_filter
  }

  /** Applies rewriting rule F8 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param op
    * @return Some BGPFilter operator if `term` was a BGPFilter with a single Pattern with only bound variables.
    */
  def F8(op: BGPFilter): Option[BGPFilter] = {
    val patterns = op.patterns
    val in = op.inputs.head
    val out = op.outputs.head

    if (groupedSchemaEarlyAbort(op.inputSchema)) {
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

    val internalPipeName = generate()
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

    Rewriter.fixReplacementwithMultipleOperators(op, group_filter.get, other_filter.get)

    group_filter foreach {
      Rewriter.connect(_, other_filter.get)
    }

    if (group_filter.isDefined) {
      in.removeConsumer(op)
    }

    group_filter
  }

  /** Applies rewriting rule F9 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    */
  def F9(op: BGPFilter): Option[BGPFilter] = {
    val patterns = op.patterns
    val in = op.inputs.head
    val out = op.outputs.head

    if (patterns.length < 2) {
      return None
    }

    if (RDF.isPathJoin(patterns) || RDF.isStarJoin(patterns)) {
      return None
    }

    val new_filters: Seq[BGPFilter] = patterns map { p: TriplePattern =>
      BGPFilter(Pipe(generate()), Pipe(generate()), List(p))
    }

    Functions.newFlowIgnoringOld(new_filters:_*)
    Some(fixReplacementwithMultipleOperators(op, new_filters.head, new_filters.last))
  }

  /** Applies rewriting rule J1 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param op
    * @return Some BGPFilter objects if the input filters BGP is a star join.
    */
  def J1(op: BGPFilter): Option[List[BGPFilter]] = {
    val out = op.outputs.head
    val in = op.inputs.head
    val patterns = op.patterns

    def isNamed(r: Ref): Option[NamedField] = r match {
      case n@NamedField(_, _) => Some(n)
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

    // This maps NamedField to a list of pipe names columns. Each column of that specific Pipe (produces by one of
    // the filters) contains the value of the NamedField in the join.
    // Its keys are also all the NamedFields that appear in `patterns`.
    val namedFieldToPipeName: Map[NamedField, List[(String, Column.Column)]] = Map.empty

    val filters = patterns map { p =>
      val pipename = generate()

      val namedFieldsOfP = RDF.namedFieldColumnPairFromPattern(p)
      namedFieldsOfP foreach { case (nf, c) =>
        namedFieldToPipeName(nf) = namedFieldToPipeName.getOrElse(nf, List.empty) :+(pipename, c)
      }

      BGPFilter(Pipe(pipename), in, List(p))
    }

    val joinOutPipeName = generate()

    // The NamedField that we're joining on and its position in the patterns
    val starJoinFieldName = RDF.starJoinColumn(patterns).get._2
    val starJoinColumnName = Column.columnToNamedField(namedFieldToPipeName(starJoinFieldName).head._2)

    val join = Join(Pipe(joinOutPipeName),
      filters map { f => Pipe(f.outPipeName, f) },
      // Use map here to make sure the amount of field expressions is the same as the amount of filters
      filters map { _ => List(starJoinColumnName) })

    filters foreach { f =>
      f.outputs.head.consumer = List(join)
      f.constructSchema
    }

    val generators = namedFieldToPipeName.toSeq.sortBy(_._1.name).map { case (nf, (firstSourceName, firstSourceColumn) :: _) =>
      GeneratorExpr(
        RefExpr(
          NamedField(Column.columnToNamedField(firstSourceColumn).name, List(firstSourceName))),
        Some(Field(nf.name, Types.CharArrayType)))
    } toList

    val foreach = Foreach(out, Pipe(joinOutPipeName, join),
      GeneratorList(
        generators
      )
    )
    foreach.constructSchema

    Rewriter.connect(join, foreach)
    Rewriter.replaceOpInSuccessorsInputs(op, foreach)

    Some(filters)
  }

  /** Applies rewriting rule J2 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    */
  def J2(op: BGPFilter): Option[(Foreach, Filter)] = {
    val patterns = op.patterns
    if (!RDF.isStarJoin(patterns)) {
      return None
    }

    val out = op.outputs.head
    val in = op.inputs.head

    // We'll reuse in later on, so we need to remove `op` from its consumers
    in.removeConsumer(op)

    val internalPipeName = generate()

    val filters: List[Filter] = patterns map RDF.patternToConstraint flatMap { c =>
      Some(Filter(Pipe(generate()), Pipe("stmts"), c.get))
    }

    val filterPipeNames = filters map (_.outputs.head.name)

    // This generates the GENERATE *, COUNT(t1) AS cnt1, ..., COUNT(tN) as cntN; operator
    val countAsOps: List[GeneratorExpr] = filterPipeNames.zipWithIndex.map { case (name, i) =>
      GeneratorExpr(Func("COUNT",
        List(RefExpr(NamedField(name)))),
        Some(Field(s"cnt$i", Types.ByteArrayType)))
    }

    val generatorOps: List[PigOperator] = filters :+ Generate(
      GeneratorExpr(RefExpr(NamedField("*"))) :: countAsOps)

    val foreach =
      Foreach(Pipe(internalPipeName), Pipe(in.name), GeneratorPlan(generatorOps))

    val countGtZeroConstraint: Predicate = filterPipeNames.zipWithIndex.map { case (_, i) =>
      Gt(RefExpr(NamedField(s"cnt$i")), RefExpr(Value(0)))
    } reduceLeft And

    val filter = Filter(out, Pipe(internalPipeName, foreach), countGtZeroConstraint)

    Rewriter.connect(foreach, filter)

    Some((foreach, filter))
  }

  /** Applies rewriting rule J3 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param op
    * @return Some BGPFilter objects if the input filters BGP is a star join.
    */
  def J3(op: BGPFilter): Option[List[PigOperator]] = {
      val out = op.outputs.head
      val in = op.inputs.head
      val patterns = op.patterns
      if (!RDF.isPathJoin(patterns)) {
        return None
      }

      // We'll reuse in later on, so we need to remove `op` from its consumers
      in.removeConsumer(op)

      val pathJoinField = RDF.pathJoinNamedField(patterns).get

      // This maps NamedField to a list of pipe names and columns. Each column of that specific Pipe (produces by one of
      // the filters) contains the value of the NamedField in the join.
      // Its keys are also all the NamedFields that appear in `patterns`.
      val namedFieldToPipeName: Map[NamedField, List[(String, Column.Column)]] = Map.empty

      // First build new BGPFilter objects for all the patterns.

      val newBGPFilters = patterns map { p =>
        val pipename = generate()

        // TODO this is duplicated from J1
        val namedFieldsOfP = RDF.namedFieldColumnPairFromPattern(p)
        namedFieldsOfP foreach { case (nf, c) =>
          namedFieldToPipeName(nf) = namedFieldToPipeName.getOrElse(nf, List.empty) :+(pipename, c)
        }

        BGPFilter(Pipe(pipename), in, List(p))
      }

      // Now we need to join them again. Note: in the paper, multiple JOINs are used, but just do it in one here
      // Since each filters schema is still (s,p,o), we can't join by namedfields, but only but s, p or o.
      def findColumnForNamedField(nf: NamedField, p: TriplePattern): NamedField = p match {
        case TriplePattern(n, _, _) if n == nf => Column.columnToNamedField(Column.Subject)
        case TriplePattern(_, n, _) if n == nf => Column.columnToNamedField(Column.Predicate)
        case TriplePattern(_, _, n) if n == nf => Column.columnToNamedField(Column.Object)
      }

      val joinFields = patterns map { p =>
        List(findColumnForNamedField(pathJoinField, p))
      }

      val joinOutPipeName = generate()

      val join = Join(Pipe(joinOutPipeName),
        newBGPFilters map { f => Pipe(f.outPipeName, f)},
        joinFields
      )

      // Set newBGPFilters' outputs to the join
      newBGPFilters foreach { f =>
        f.outputs.head.consumer = List(join)
        f.constructSchema
      }

      // TODO this is duplicated from J1
      val generators = namedFieldToPipeName.toSeq.sortBy(_._1.name).map { case (nf, (firstSourceName, firstSourceColumn) :: _) =>
        GeneratorExpr(
          RefExpr(
            NamedField(Column.columnToNamedField(firstSourceColumn).name, List(firstSourceName))),
          Some(Field(nf.name, Types.CharArrayType)))
      } toList

      val foreach = Foreach(out, Pipe(joinOutPipeName, join),
        GeneratorList(
          generators
        )
      )

      Rewriter.connect(join, foreach)
      Rewriter.replaceOpInSuccessorsInputs(op, foreach)

      foreach.constructSchema

      Some(newBGPFilters)
  }

  /** Applies rewriting rule J4 of the paper "[[http://www.btw-2015.de/res/proceedings/Hauptband/Wiss/Hagedorn-SPARQling_Pig_-_Processin.pdf SPARQling Pig - Processing Linked Data with Pig Latin]].
    *
    * @param op
    * @return Some BGPFilter objects if the input filters BGP is a star join.
    */
  def J4(op: BGPFilter): Option[List[PigOperator]] = {
      val out = op.outputs.head
      val in = op.inputs.head
      val patterns = op.patterns

      if (!RDF.isPathJoin(patterns)) {
        return None
      }

      // We'll reuse in later on, so we need to remove `op` from its consumers
      in.removeConsumer(op)

      val pathJoinField = RDF.pathJoinNamedField(patterns).get

      // This maps NamedField to a list of pipe names and columns. Each column of that specific Pipe (produces by one of
      // the filters) contains the value of the NamedField in the join.
      // Its keys are also all the NamedFields that appear in `patterns`.
      val namedFieldToPipeName: Map[NamedField, List[(String, Column.Column)]] = Map.empty

      // First build new BGPFilter objects for all the patterns.
      val newBGPFilters = patterns map { p =>
        val pipename = generate()
        val f = BGPFilter(Pipe(pipename), in, List(p))
        f.constructSchema
        f
      }

      // Now build the foreach statements that flatten the filters outputs
      val flattening_foreachs = newBGPFilters map { f =>
        val pipename = generate()
        // TODO extract this to a function, there are only 3 cases anyway
        val fo = Foreach(
          Pipe(pipename),
          Pipe(f.outPipeName),
          GeneratorList(
            List(
              GeneratorExpr(
                RefExpr(
                  NamedField("subject"))),
              GeneratorExpr(
                FlattenExpr(
                  RefExpr(
                    NamedField("stmts"))),
                None))))

        // Every BGPFilter here has only one pattern
        val namedFieldsOfP = RDF.namedFieldColumnPairFromPattern(f.patterns.head)
        namedFieldsOfP foreach { case (nf, c) =>
          namedFieldToPipeName(nf) = namedFieldToPipeName.getOrElse(nf, List.empty) :+ (pipename, c)
        }

        Rewriter.connect(f, fo)
        fo
      }

      // Now we need to join them again. Note: in the paper, multiple JOINs are used, but just do it in one here
      // Since each filters schema is still (s,p,o), we can't join by namedfields, but only but s, p or o.
      def findColumnForNamedField(nf: NamedField, p: TriplePattern): NamedField = p match {
        case TriplePattern(n, _, _) if n == nf => Column.columnToNamedField(Column.Subject)
        case TriplePattern(_, n, _) if n == nf => Column.columnToNamedField(Column.Predicate)
        case TriplePattern(_, _, n) if n == nf => Column.columnToNamedField(Column.Object)
      }

      val joinFields = patterns map { p =>
        List(findColumnForNamedField(pathJoinField, p))
      }

      val joinOutPipeName = generate()

      val join = Join(Pipe(joinOutPipeName),
        flattening_foreachs map { fo => Pipe(fo.outPipeName, fo)},
        joinFields
      )

      // Set flattening_foreachs' outputs to the join
      flattening_foreachs foreach { fo =>
        fo.outputs.head.consumer = List(join)
        fo.constructSchema
      }

      // TODO this is duplicated from J1
      val generators = namedFieldToPipeName.toSeq.sortBy(_._1.name).map { case (nf, (firstSourceName, firstSourceColumn) :: _) =>
        GeneratorExpr(
          RefExpr(
            NamedField(Column.columnToNamedField(firstSourceColumn).name, List(firstSourceName))),
          Some(Field(nf.name, Types.CharArrayType)))
      } toList

      val foreach = Foreach(out, Pipe(joinOutPipeName, join),
        GeneratorList(
          generators
        )
      )

      Rewriter.connect(join, foreach)
      Rewriter.replaceOpInSuccessorsInputs(op, foreach)

      foreach.constructSchema

      Some(newBGPFilters)
  }

  def registerRules() = {
    Rewriter toReplace (classOf[RDFLoad]) applyRule R1
    Rewriter applyRule R2
    Rewriter toReplace (classOf[RDFLoad]) applyRule L2
    Rewriter applyRule F1
    Rewriter toReplace (classOf[BGPFilter]) applyRule F2
    Rewriter toReplace (classOf[BGPFilter]) applyRule F3
    Rewriter toReplace (classOf[BGPFilter]) applyRule F4
    Rewriter applyRule F5
    Rewriter applyRule F6
    Rewriter applyRule F7
    Rewriter applyRule F8
    Rewriter unless plainSchemaJoinEarlyAbort  applyRule J1
    Rewriter unless groupedSchemaJoinEarlyAbort applyRule J2
    Rewriter unless plainSchemaJoinEarlyAbort applyRule J3
    Rewriter unless groupedSchemaJoinEarlyAbort applyRule J4
  }
}
