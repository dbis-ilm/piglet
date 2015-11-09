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
package dbis.pig.codegen

import dbis.pig.expr.RefExprExtractor
import dbis.pig.op._
import dbis.pig.schema._
import dbis.pig.plan.DataflowPlan

import scala.collection.mutable.ArrayBuffer


class BatchGenCode(template: String) extends ScalaBackendGenCode(template) {


  /*------------------------------------------------------------------------------------------------- */
  /*                                  Scala-specific code generators                                  */
  /*------------------------------------------------------------------------------------------------- */

  /**
    *
    * @param schema
    * @param tuplePrefix
    * @return
    */
  override def emitRef(schema: Option[Schema], ref: Ref,
                       tuplePrefix: String = "t",
                       aggregate: Boolean = false,
                       namedRef: Boolean = false): String = ref match {
    case DerefTuple(r1, r2) =>
      if (aggregate)
        s"${emitRef(schema, r1, "t")}.map(e => e${emitRef(tupleSchema(schema, r1), r2, "")})"
      else
        s"${emitRef(schema, r1, "t")}${emitRef(tupleSchema(schema, r1), r2, "", aggregate, namedRef)}"
    case _ => super.emitRef(schema, ref, tuplePrefix, aggregate, namedRef)
  }

  /**
    * Generates Scala code for a nested plan, i.e. statements within nested FOREACH.
    *
    * @param parent the parent FOREACH statement
    * @param plan the dataflow plan representing the nested statements
    * @return the generated code
    */
  def emitNestedPlan(parent: PigOperator, plan: DataflowPlan): String = {
    val schema = parent.inputSchema

    require(parent.schema.isDefined)
    val className = schemaClassName(parent.schema.get.className)

    "{\n" + plan.operators.map {
      case Generate(expr) => s"""${className}(${emitGenerator(schema, expr, namedRef = true)})"""
      case n@ConstructBag(out, ref) => ref match {
        case DerefTuple(r1, r2) => {
          // there are two options of ConstructBag
          // 1. r1 refers to the input pipe of the outer operator (for automatically
          //    inserted ConstructBag operators)
          if (r1.toString == parent.inPipeName) {
            val pos = findFieldPosition(schema, r2)
            // println("pos = " + pos)
            s"""val ${n.outPipeName} = t._$pos.toList"""
          }
          else {
            // 2. r1 refers to a field in the schema
            val p1 = findFieldPosition(schema, r1)
            val p2 = findFieldPosition(tupleSchema(schema, r1), r2)
            // println("pos2 = " + p1 + ", " + p2)
            s"""val ${n.outPipeName} = t._$p1.map(l => l._$p2).toList"""
          }
        }
        case _ => "" // should not happen
      }
      case n@Distinct(out, in, _) => callST("distinct", Map("out" -> n.outPipeName, "in" -> n.inPipeName))
      case n@Filter(out, in, pred, _) => callST("filter", Map("out" -> n.outPipeName, "in" -> n.inPipeName, "pred" -> emitPredicate(n.schema, pred)))
      case n@Limit(out, in, num) => callST("limit", Map("out" -> n.outPipeName, "in" -> n.inPipeName, "num" -> num))
      case OrderBy(out, in, orderSpec, _) => "" // TODO!!!!
      case _ => ""
    }.mkString("\n") + "}"
  }

  def emitForeachExpr(node: PigOperator, gen: ForeachGenerator): String = {
    // we need to know if the generator contains flatten on tuples or on bags (which require flatMap)
    // val requiresPlainFlatten =  node.asInstanceOf[Foreach].containsFlatten(onBag = false)
    val requiresFlatMap = node.asInstanceOf[Foreach].containsFlatten(onBag = true)
    gen match {
      case GeneratorList(expr) => {
        if (requiresFlatMap)
          emitBagFlattenGenerator(node, expr)
        else
          emitGenerator(node.inputSchema, expr)
      }
      case GeneratorPlan(plan) => {
        val subPlan = node.asInstanceOf[Foreach].subPlan.get
        emitNestedPlan(node, subPlan)
      }
    }
  }

  /*------------------------------------------------------------------------------------------------- */
  /*                                   Node code generators                                           */
  /*------------------------------------------------------------------------------------------------- */

  /**
    * Generates code for the CROSS operator.
    *
    * @param node the Cross Operator node
    * @return the Scala code implementing the CROSS operator
    */

  def emitCross(node: PigOperator): String = {
    val rels = node.inputs
    callST("cross", Map("out" -> node.outPipeName, "rel1" -> rels.head.name, "rel2" -> rels.tail.map(_.name)))
  }

  /**
    * Generates code for the DISTINCT Operator
    *
    * @param node the DISTINCT operator
    * @return the Scala code implementing the DISTINCT operator
    */
  def emitDistinct(node: PigOperator): String = {
    callST("distinct", Map("out" -> node.outPipeName, "in" -> node.inPipeName))
  }

  /**
    * Generates code for the FILTER Operator
    *
    * @param node the FILTER operator
    * @param pred the filter predicate
    * @return the Scala code implementing the FILTER operator
    */
  def emitFilter(node: PigOperator, pred: Predicate): String = {
    callST("filter", Map("out" -> node.outPipeName, "in" -> node.inPipeName,
      "pred" -> emitPredicate(node.schema, pred)))
  }

  /**
    * Generates code for the FOREACH Operator
    *
    * @param node the FOREACH Operator node
    * @param gen the generate expression
    * @return the Scala code implementing the FOREACH operator
    */
  def emitForeach(node: PigOperator, gen: ForeachGenerator): String = {
    require(node.schema.isDefined)
    val className = schemaClassName(node.schema.get.className)
    val expr = emitForeachExpr(node, gen)
    // in case of a nested FOREACH the tuples are creates as part of the GENERATE clause
    // -> no need to give the schema class
    if (gen.isInstanceOf[GeneratorPlan]) {
      callST("foreachNested", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "expr" -> expr))
    }
    else {
      // we need to know if the generator contains flatten on tuples or on bags (which require flatMap)
      val requiresFlatMap = node.asInstanceOf[Foreach].containsFlatten(onBag = true)
      if (requiresFlatMap)
        callST("foreachFlatMap", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "expr" -> expr))
      else
        callST("foreach", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "class" -> className, "expr" -> expr))
    }
  }

  /**
    * Generates code for the GROUPING Operator
    *
    * @param node the GROUPING operator
    * @param groupExpr the grouping expression
    * @return the Scala code implementing the GROUPING operator
    */
  def emitGrouping(node: Grouping, groupExpr: GroupingExpression): String = {
    require(node.schema.isDefined)
    val className = schemaClassName(node.schema.get.className)

    // GROUP ALL: no need to generate a key
    if (groupExpr.keyList.isEmpty)
      callST("groupBy", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "class" -> className))
    else {
      val keyExtr = if (groupExpr.keyList.size > 1) {
        // the grouping key consists of multiple fields, i.e. we have
        // to construct a tuple where the type is the TupleType of the group field
        val field = node.schema.get.field("group")
        val className = field.fType match {
          case TupleType(f, c) => schemaClassName(c)
          case _ => throw TemplateException("unknown type for grouping key")
        }

        s"${className}(" + (for (i <- 1 to groupExpr.keyList.size) yield s"k._$i").mkString(", ") + ")"
      }
      else "k" // the simple case: the key is a single field

      callST("groupBy", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "class" -> className,
        "expr" -> emitGroupExpr(node.inputSchema, groupExpr),
        "keyExtr" -> keyExtr))
    }
  }

  /**
    * Generates code for the JOIN operator.
    *
    * @param node the JOIN operator node
    * @param rels list of Pipes to join
    * @param exprs list of join keys
    * @return the Scala code implementing the JOIN operator
    */
  def emitJoin(node: PigOperator, rels: List[Pipe], exprs: List[List[Ref]]): String = {
    require(node.schema.isDefined)

    val res = node.inputs.zip(exprs)
    val keys = res.map { case (i, k) => emitJoinKey(i.producer.schema, k) }

    /*
     * We don't generate key-value RDDs which we have already created and registered in joinKeyVars.
     * Thus, we build a list of 1 and 0's where 1 stands for a relation name for which we have already
     * created a _kv variable.
     */
    val duplicates = rels.map(r => if (joinKeyVars.contains(r.name)) 1 else 0)

    /*
     * Now we build lists for rels and keys by removing the elements corresponding to 1's in the duplicate
     * list.
     */
    val drels = rels.zipWithIndex.filter { r => duplicates(r._2) == 0 }.map(_._1)
    val dkeys = keys.zipWithIndex.filter { k => duplicates(k._2) == 0 }.map(_._1)

    /*
     * And finally, create the join kv vars for them...
     */
    var str = callST("join_key_map", Map("rels" -> drels.map(_.name), "keys" -> dkeys))

    /*
     * We construct a string v._0, v._1 ... w._0, w._1 ...
     * The numbers of v's and w's are determined by the size of the input schemas.
     */
    val className = node.schema match {
      case Some(s) => schemaClassName(s.className)
      case None => schemaClassName(node.outPipeName)
    }

    /*
      *  ...as well as the actual join.
      */
    if (rels.length == 2) {
      val vsize = rels.head.inputSchema.get.fields.length
      val fieldList = node.schema.get.fields.zipWithIndex
        .map { case (f, i) => if (i < vsize) s"v._$i" else s"w._${i - vsize}" }.mkString(", ")

      str += callST("join",
        Map("out" -> node.outPipeName,
          "rel1" -> rels.head.name,
          "class" -> className,
          "rel2" -> rels.tail.map(_.name),
          "fields" -> fieldList))
    }
    else {
      var pairs = "(v1,v2)"
      for (i <- 3 to rels.length) {
        pairs = s"($pairs,v$i)"
      }
      val fieldList = ArrayBuffer[String]()
      for (i <- 1 to node.inputs.length) {
        node.inputs(i - 1).producer.schema match {
          case Some(s) => fieldList ++= s.fields.zipWithIndex.map { case (f, k) => s"v$i._$k" }
          case None => fieldList += s"v$i._0"
        }
      }

      str += callST("m_join",
        Map("out" -> node.outPipeName,
          "rel1" -> rels.head.name,
          "class" -> className,
          "rel2" -> rels.tail.map(_.name),
          "pairs" -> pairs,
          "fields" -> fieldList.mkString(", ")))
    }
    joinKeyVars += rels.head.name
    joinKeyVars ++= rels.tail.map(_.name)
    str
  }

  /**
    * Generates code for the LIMIT operator
    *
    * @param node the LIMIT operator
    * @param num number of returned records
    * @return the Scala code implementing the LIMIT operator
    */
  def emitLimit(node: PigOperator, num: Int): String = {
    callST("limit", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "num" -> num))
  }

  /**
    * Generates code for the ORDERBYoOperator
    *
    * @param node the OrderBy operator node
    * @param spec Order specification
    * @return the Scala code implementing the ORDERBY operator
    */
  def emitOrderBy(node: PigOperator, spec: List[OrderBySpec]): String = {
    val key = emitSortKey(node.schema, spec, node.outPipeName, node.inPipeName)
    val asc = ascendingSortOrder(spec.head)
    callST("orderBy", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "key" -> key, "asc" -> asc))
  }


  private def callsAverageFunc(node: PigOperator, e: Expr): Boolean =
    e.traverseOr(node.inputSchema.getOrElse(null), Expr.containsAverageFunc)

  /**
    * Generates code for the ACCUMULATE operator
    *
    * @param node the ACCUMULATE operator
    * @param gen the generator expressions containing the aggregates
    * @return the Scala code implementing the operator
    */
  def emitAccumulate(node: PigOperator, gen: GeneratorList): String = {
    require(node.schema.isDefined)
    val outClassName = schemaClassName(node.schema.get.className)
    val helperClassName = s"_${node.schema.get.className}_HelperTuple"

    // generate update expression
    val updExpr = gen.exprs.zipWithIndex.map { case (e, i) =>
      // AVG requires special handling
      if (callsAverageFunc(node, e.expr))
        s"PigFuncs.incrSUM(acc._${i}sum, v._${i}sum), PigFuncs.incrCOUNT(acc._${i}cnt, v._${i}cnt)"
      else {
        require(e.expr.isInstanceOf[Func])
        val func = e.expr.asInstanceOf[Func]
        val funcName = func.f.toUpperCase
        s"PigFuncs.incr${funcName}(acc._$i, v._$i)"
      }
    }.mkString(", ")

    // generate init expression
    val initExpr = gen.exprs.map { e =>
      // For each expression in GENERATE we have to extract the referenced fields.
      // So, we extract all RefExprs.
      val traverse = new RefExprExtractor
      e.expr.traverseAnd(null, traverse.collectRefExprs)
      val refExpr = traverse.exprs.head
      val str = refExpr.r match {
        case nf@NamedField(n, _) => s"t._${node.inputSchema.get.indexOfField(nf)}"
        case PositionalField(p) => s"t._$p"
        case _ => ""
      }
      // in case of AVERAGE we need fields for SUM and COUNT
      if (callsAverageFunc(node, e.expr)) s"${str}, ${str}" else str
    }.mkString(", ")

    // generate final aggregation expression
    val aggrExpr = gen.exprs.zipWithIndex.map { case (e, i) =>
      // For each expression we need to know the corresponding field in the helper tuple.
      // But because we assume that the expressions are only aggregate functions, we have to
      // distinguish only between avg and other functions.
      if (callsAverageFunc(node, e.expr))
        s"${node.outPipeName}_fold._${i}sum / ${node.outPipeName}_fold._${i}cnt"
      else
        s"${node.outPipeName}_fold._$i"
    }.mkString(", ")

    var res = callST("accumulate_aggr", Map("out" -> node.outPipeName,
      "helper_class" -> helperClassName,
      "expr" -> updExpr))
    res += "\n"
    res += callST("accumulate", Map("out" -> node.outPipeName,
      "in" -> node.inPipeName,
      "helper_class" -> helperClassName,
      "class" -> outClassName,
      "init_expr" -> initExpr,
      "aggr_expr" -> aggrExpr))
    res
  }

  /*------------------------------------------------------------------------------------------------- */
  /*                           implementation of the GenCodeBase interface                            */
  /*------------------------------------------------------------------------------------------------- */

  /**
    * Generate code for the given Pig operator.
    *
    * @param node the operator (an instance of PigOperator)
    * @return a string representing the code
    */
  override def emitNode(node: PigOperator): String = {
    node match {
      /*
       * NOTE: Don't use "out" here -> it refers only to initial constructor argument but isn't consistent
       *       after changing the pipe name. Instead, use node.outPipeName
       */
      case Cross(out, rels, _) => emitCross(node)
      case Distinct(out, in, _) => emitDistinct(node)
      case Filter(out, in, pred, _) => emitFilter(node, pred)
      case Foreach(out, in, gen, _) => emitForeach(node, gen)
      case op@Grouping(out, in, groupExpr, _) => emitGrouping(op, groupExpr)
      case Join(out, rels, exprs, _) => emitJoin(node, node.inputs, exprs)
      case Limit(out, in, num) => emitLimit(node, num)
      case OrderBy(out, in, orderSpec, _) => emitOrderBy(node, orderSpec)
      case Accumulate(out, in, gen) => emitAccumulate(node, gen)
      case _ => super.emitNode(node)
    }
  }


  override def emitHelperClass(node: PigOperator): String = {
    node match {
      case op@Accumulate(out, in, gen) => {
        // for ACCUMULATE we need a special tuple class
        val inSchemaClassName = schemaClassName(op.inputSchema.get.className)
        val fieldStr = s"_t: ${inSchemaClassName} = null, " + op.generator.exprs.zipWithIndex.map{ case (e, i) =>
          if (callsAverageFunc(node, e.expr)) {
            // TODO: determine type
            val inType = "Int"
            s"_${i}sum: ${inType} = 0, _${i}cnt: Int = 0"
          }
          else {
            val resType = e.expr.resultType(op.inputSchema)
            val defaultValue = if (Types.isNumericType(resType)) "0" else "null"
            s"_$i: ${scalaTypeMappingTable(resType)} = ${defaultValue}"
          }
        }.mkString(", ")
        val toStr = "\"\""
        callST("schema_class", Map("name" -> s"_${op.schema.get.className}_HelperTuple",
          "fields" -> fieldStr,
          "string_rep" -> toStr))

      }
      case _ => super.emitHelperClass(node)
    }
  }

}

class BatchCompile(templateFile: String) extends Compile {
  override val codeGen = new BatchGenCode(templateFile)
}

