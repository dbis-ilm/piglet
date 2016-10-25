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
package dbis.piglet.codegen.flink

import dbis.piglet.op._
import dbis.piglet.expr._
import dbis.piglet.udf._
import dbis.piglet.schema._
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.backends.BackendManager
import scala.collection.mutable.ListBuffer
import java.nio.file.Path
import dbis.piglet.codegen.{CodeGenContext, ScalaBackendCodeGen, CodeGenerator}
import scala.collection.mutable
import scala.collection.mutable.{ Map => MMap }
import scala.collection.mutable.ArrayBuffer

class FlinkStreamingCodeGen(template: String) extends ScalaBackendCodeGen(template) {

  /*------------------------------------------------------------------------------------------------- */
  /*                                  Scala-specific code generators                                  */
  /*------------------------------------------------------------------------------------------------- */

  def containsAggregates(gen: ForeachGenerator): Boolean = {
    var exprs = gen match {
      case GeneratorList(expr) => expr
      case GeneratorPlan(plan) => plan.last.asInstanceOf[Generate].exprs
    }
    exprs.foreach { e =>
      e.expr match {
        case Func(f, _) => UDFTable.findFirstUDF(f) match {
          case Some(udf) if udf.isAggregate => return true
          case _ =>
        }
        case _ =>
      }
    }
    return false
  }

  /**
    *
    * @param schema
    * @param ref
    * @param tuplePrefix
    * @return
    */
  override def emitRef(ctx: CodeGenContext, ref: Ref): String = ref match {
    case DerefTuple(r1, r2) =>
    if (ctx.aggregate)
      s"${emitRef(CodeGenContext(schema = ctx.schema, tuplePrefix = "t"), r1)}.map(e => e${emitRef(CodeGenContext(schema = tupleSchema(ctx.schema, r1), tuplePrefix = ""), r2)})"
    else
      s"${emitRef(CodeGenContext(schema = ctx.schema, tuplePrefix = "t"), r1)}${emitRef(CodeGenContext(schema = tupleSchema(ctx.schema, r1), tuplePrefix = "", aggregate = ctx.aggregate, namedRef = ctx.namedRef), r2)}"
    case _ => super.emitRef(ctx, ref)
  }

  override def emitHelperClass(node: PigOperator): String = {
    //require(node.schema.isDefined)
    def emitHelperAccumulate(node: PigOperator, gen: GeneratorList): String = {
      val inSchemaClassName = node.inputSchema match {
        case Some(s) => schemaClassName(s.className)
        case None    => "Record"
      }
      val fieldStr = s"_t: ${inSchemaClassName} = null, " + gen.exprs.filter(e => e.expr.isInstanceOf[Func]).zipWithIndex.map {
        case (e, i) =>
          if (e.expr.traverseOr(node.inputSchema.getOrElse(null), Expr.containsAverageFunc)) {
            val inType = "Long"
            s"_${i}sum: ${inType} = 0, _${i}cnt: Long = 0"
          } else {
            val resType = e.expr.resultType(node.inputSchema)
            val funcName = e.expr.asInstanceOf[Func].f.toUpperCase
            val defaultValue = if (Types.isNumericType(resType)) funcName match {
              // for min and max we need special initial values
              case "MIN" => "Int.MaxValue"
              case "MAX" => "Int.MinValue"
              case _     => 0
            }
            else "null"
            s"_$i: ${scalaTypeMappingTable(resType)} = ${defaultValue}"
          }
      }.mkString(", ")
      val schemaHelper = callST("schema_class", Map("name" -> s"_${node.schema.get.className}_HelperTuple",
        "fields" -> fieldStr,
        "string_rep" -> "\"\""))

      val expr = gen.exprs.filter(e => e.expr.isInstanceOf[Func]).zipWithIndex.map {
        case (e, i) =>
          if (e.expr.traverseOr(node.inputSchema.getOrElse(null), Expr.containsAverageFunc))
            s"PigFuncs.incrSUM(acc._${i}sum, v._${i}sum), PigFuncs.incrCOUNT(acc._${i}cnt, v._${i}cnt)"
          else {
            val func = e.expr.asInstanceOf[Func]
            val funcName = func.f.toUpperCase
            s"PigFuncs.incr${funcName}(acc._$i, v._$i)"
          }
      }.mkString(", ")

      // generate init expression
      val initExpr = gen.exprs.filter(e => e.expr.isInstanceOf[Func]).map { e =>
        // For each expression in GENERATE we have to extract the referenced fields.
        // So, we extract all RefExprs.
        val traverse = new RefExprExtractor
        e.expr.traverseAnd(null, traverse.collectRefExprs)
        val refExpr = traverse.exprs.head
        // in case of COUNT we simply pass 0 instead of the field: this allows COUNT on chararray
        if (e.expr.traverseOr(node.inputSchema.getOrElse(null), Expr.containsCountFunc))
          "0"
        else {
          val str = refExpr.r match {
            case nf @ NamedField(n, _) => s"t._${node.inputSchema.get.indexOfField(nf)}"
            case PositionalField(p)    => if (node.inputSchema.isDefined) s"t._$p" else s"t.get(0)"
            case DerefTuple(r1, r2)    => emitRef(CodeGenContext(schema = node.inputSchema, tuplePrefix = "t"), r1) + ".head" + emitRef(CodeGenContext(schema = tupleSchema(node.inputSchema, r1), tuplePrefix = ""), r2)
            case _                     => ""
          }
          // in case of AVERAGE we need fields for SUM and COUNT
          if (e.expr.traverseOr(node.inputSchema.getOrElse(null), Expr.containsAverageFunc)) s"${str}, ${str}" else str
        }
      }.mkString(", ")

      val streamFunc = callST("accumulate_aggr", Map("out" -> node.outPipeName,
        "helper_class" -> s"_${node.schema.get.className}_HelperTuple",
        "class" -> schemaClassName(node.inputSchema.get.className),
        "expr" -> expr,
        "init_expr" -> initExpr))
      schemaHelper + "\n" + streamFunc
    }
    node match {
      // Construct Apply method for window evaluation
      case WindowApply(out, in, fname) => {
        val inSchema = schemaClassName(node.inputSchema.get.className)
        val outSchema = schemaClassName(node.schema.get.className)
        var fname, applyBody = ""
        var lastOp: PigOperator = new Empty(Pipe("empty"))
        val littleWalker = mutable.Queue(node.inputs.head.producer.outputs.flatMap(_.consumer).toSeq: _*)
        while (!littleWalker.isEmpty) {
          val operator = littleWalker.dequeue()
          operator match {
            case o @ Filter(_, _, pred, windowMode) if (windowMode) => {
              val predicate = emitPredicate(CodeGenContext(schema = o.schema), pred)
              applyBody += callST("filterHelper", Map("pred" -> predicate)) + "\n"
            }
            case o @ Distinct(_, _, windowMode) if (windowMode) => {
              applyBody += callST("distinctHelper") + "\n"
            }
            case o @ OrderBy(_, _, spec, windowMode) if (windowMode) => {
              var params = Map[String, Any]()
              params += "key" -> emitSortKey(CodeGenContext(schema = o.schema), spec, o.outPipeName, o.inPipeName)
              params += "ordering" -> emitOrdering(o.schema, spec)
              applyBody += callST("orderByHelper", Map("params" -> params)) + "\n"
            }
            case o @ Grouping(_, _, groupExpr, windowMode) if (windowMode) => {
              var params = Map[String, Any]()
              params += "expr" -> emitGroupExpr(CodeGenContext(schema = node.inputSchema), groupExpr)
              params += "class" -> schemaClassName(o.schema.get.className)
              applyBody += callST("groupByHelper", Map("params" -> params)) + "\n"
            }
            case o @ Foreach(_, _, gen, windowMode) if (windowMode) => {
              fname = "WindowFunc" + o.outPipeName
              var params = Map[String, Any]()
              params += "expr" -> emitForeachExpr(o, gen, false)
              if (!gen.isNested) params += "class" -> schemaClassName(o.schema.get.className)
              applyBody += callST("foreachHelper", Map("params" -> params))
              return s"""  def ${fname}(wi: Window, ts: Iterable[${inSchema}], out: Collector[${outSchema}]) = {
                |    ts
                |${applyBody}
                |  }
                """.stripMargin
            }
            case _ =>
          }
          littleWalker ++= operator.outputs.flatMap(_.consumer)
          if (littleWalker.isEmpty) lastOp = operator
        }
        val before = lastOp.inputs.tail.head
        fname = "WindowFunc" + before.name
        var params = Map[String, Any]()
        params += "expr" -> "t"
        params += "class" -> ""
        applyBody += callST("foreachHelper", Map("params" -> params))
        s"""  def ${fname}(wi: Window, ts: Iterable[${inSchema}], out: Collector[${outSchema}]) = {
          |    ts
          |${applyBody}
          |  }
          """.stripMargin

      }
      case op @ Accumulate(out, in, gen) => emitHelperAccumulate(op, gen)
      case op @ Foreach(out, in, gen, windowMode) if (!windowMode && containsAggregates(gen)) => {
        val genL: GeneratorList = gen match {
          case gl @ GeneratorList(expr) => gl
          case GeneratorPlan(plan)      => GeneratorList(plan.last.asInstanceOf[Generate].exprs)
        }
        emitHelperAccumulate(op, genL)
      }
      case _ => super.emitHelperClass(node)
    }
  }

  /**
   * Generates Scala code for a nested plan, i.e. statements within nested FOREACH.
   *
   * @param schema the input schema of the FOREACH statement
   * @param plan the dataflow plan representing the nested statements
   * @param aggr generate clause contains aggregates
   * @return the generated code
   */
  def emitNestedPlan(parent: PigOperator, plan: DataflowPlan, aggr: Boolean): String = {
    val schema = parent.inputSchema

    require(parent.schema.isDefined)
    val className = schemaClassName(parent.schema.get.className)

    "{\n" + plan.operators.map {
      case n @ Generate(expr) => if (aggr) emitAccumulate(n, GeneratorList(expr)) else s"""${className}(${emitGenerator(CodeGenContext(schema = schema, namedRef = true), expr)})"""
      case n @ ConstructBag(out, ref) => ref match {
        case DerefTuple(r1, r2) => {
          // there are two options of ConstructBag
          // 1. r1 refers to the input pipe of the outer operator (for automatically
          //    inserted ConstructBag operators)
          if (r1.toString == parent.inPipeName) {
            val pos = findFieldPosition(schema, r2)
            // println("pos = " + pos)
            s"""val ${n.outPipeName} = t._$pos.toList"""
          } else {
            // 2. r1 refers to a field in the schema
            val p1 = findFieldPosition(schema, r1)
            val p2 = findFieldPosition(tupleSchema(schema, r1), r2)
            // println("pos2 = " + p1 + ", " + p2)
            s"""val ${n.outPipeName} = t._$p1.map(l => l._$p2).toList"""
          }
        }
        case _ => "" // should not happen
      }
      case n @ Distinct(out, in, windowMode)       => s"""val ${n.outPipeName} = ${n.inPipeName}.distinct"""
      case n @ Filter(out, in, pred, windowMode)   => callST("filter", Map("out" -> n.outPipeName, "in" -> n.inPipeName, "pred" -> emitPredicate(CodeGenContext(schema = n.schema), pred), "windowMode" -> windowMode))
      case OrderBy(out, in, orderSpec, windowMode) => "" // TODO!!!!
      case _                                       => ""
    }.mkString("\n") + "}"
  }

  def emitForeachExpr(node: PigOperator, gen: ForeachGenerator, aggr: Boolean): String = {
    // we need to know if the generator contains flatten on tuples or on bags (which require flatMap)
    val requiresPlainFlatten = node.asInstanceOf[Foreach].containsFlatten(onBag = false)
    val requiresFlatMap = node.asInstanceOf[Foreach].containsFlatten(onBag = true)
    gen match {
      case gl @ GeneratorList(expr) => {
        if (requiresFlatMap) emitBagFlattenGenerator(node, expr)
        else {
          if (aggr) emitAccumulate(node, gl)
          else emitGenerator(CodeGenContext(schema = node.inputSchema), expr)
        }
      }
      case GeneratorPlan(plan) => {
        val subPlan = node.asInstanceOf[Foreach].subPlan.get
        emitNestedPlan(node, subPlan, aggr)
      }
    }
  }

  def emitStageIdentifier(line: Int, lineage: String): String = ???

  def schemaExtractor(prefix: String, schema: Schema): String =
    List.range(0, schema.fields.length).map { i => s"${prefix}._$i" }.mkString(", ")
  //schema.fields.zipWithIndex.map{case (_, i) => s"${prefix}._$i"}.mkString(", ")

  /**
   * Determines the resulting field list and joined pairs for cross and join operators.
   *
   * @param node the Join or Cross node
   * @return the pairs and fields as a Tuple2[String, String]
   */
  def emitJoinFieldList(node: PigOperator): (String, String) = {
    val rels = node.inputs
    var fields = ""
    var pairs = "(v,w)"
    if (rels.length == 2) {
      val vsize = rels.head.inputSchema.get.fields.length
      fields = node.schema.get.fields.zipWithIndex
        .map { case (f, i) => if (i < vsize) s"v._$i" else s"w._${i - vsize}" }.mkString(", ")
    } else {
      pairs = "(v1,v2)"
      for (i <- 3 to rels.length) {
        pairs = s"($pairs,v$i)"
      }
      val fieldList = ArrayBuffer[String]()
      for (i <- 1 to node.inputs.length) {
        node.inputs(i - 1).producer.schema match {
          case Some(s) => fieldList ++= s.fields.zipWithIndex.map { case (f, k) => s"v$i._$k" }
          case None    => fieldList += s"v$i._0"
        }
      }
      fields = fieldList.mkString(", ")
    }
    (pairs, fields)
  }

  override def emitSortKey(ctx: CodeGenContext, orderSpec: List[OrderBySpec], out: String, in: String): String = {
    if (orderSpec.size == 1)
      emitRef(ctx, orderSpec.head.field)
    else
      s"(${orderSpec.map(r => emitRef(ctx, r.field)).mkString(",")})"
  }

  /**
   * Creates the ordering definition for a given spec and schema.
   *
   * @param schema the schema of the order by operator
   * @param orderSpec the order specifications
   * @return the ordering definition
   */
  def emitOrdering(schema: Option[Schema], orderSpec: List[OrderBySpec]): String = {

    def emitOrderSpec(spec: OrderBySpec): String = {
      val reverse = if (spec.dir == OrderByDirection.DescendingOrder) ".reverse" else ""
      s"Ordering.${scalaTypeOfField(spec.field, schema)}" + reverse
    }

    if (orderSpec.size == 1)
      emitOrderSpec(orderSpec.head)
    else
      s"Ordering.Tuple${orderSpec.size}(" + orderSpec.map { r => emitOrderSpec(r) }.mkString(",") + ")"
  }

  /*------------------------------------------------------------------------------------------------- */
  /*                                   Node code generators                                           */
  /*------------------------------------------------------------------------------------------------- */

  /**
   * Generates code for the ACCUMULATE operator
   *
   * @param node the ACCUMULATE operator
   * @param gen the generator expressions containing the aggregates
   * @return the Scala code implementing the operator
   */
  def emitAccumulate(node: PigOperator, gen: GeneratorList): String = {
    val inputSchemaDefined = node.inputSchema.isDefined
    require(node.schema.isDefined)
    val outClassName = schemaClassName(node.schema.get.className)
    val helperClassName = s"_${node.schema.get.className}_HelperTuple"
    // Check if input is already keyed/grouped, as it is necessary to use for mapWithState()
    val keyed = inputSchemaDefined && node.inputSchema.get.fields.size == 2 && node.inputSchema.get.field(0).name == "group"

    // generate final aggregation expression
    var aggrCounter = -1
    val aggrExpr = gen.exprs.map { e =>
      if (e.expr.isInstanceOf[Func]) {
        aggrCounter += 1
        if (e.expr.traverseOr(node.inputSchema.getOrElse(null), Expr.containsAverageFunc))
          s"t._${aggrCounter}sum.toDouble / t._${aggrCounter}cnt.toDouble"
        else
          s"t._$aggrCounter"
      } else
        "t._t" + s"${emitExpr(CodeGenContext(schema = node.inputSchema), e.expr)}".drop(1)
    }.mkString(", ")
    val params = MMap[String, Any]()
    params += "out" -> node.outPipeName
    params += "in" -> node.inPipeName
    params += "helper_class" -> helperClassName
    params += "class" -> outClassName
    params += "aggr_expr" -> aggrExpr
    if (!keyed) params += "notKeyed" -> true

    callST("accumulate", params.toMap)
  }

  /**
   * Generates code for the CROSS operator.
   *
   * @param node the CROSS operator node
   * @param window window information for Cross' on streams
   * @return the Scala code implementing the CROSS operator
   */
  def emitCross(node: PigOperator, window: Tuple2[Int, String]): String = {
    val rels = node.inputs
    val className = node.schema match {
      case Some(s) => schemaClassName(s.className)
      case None    => schemaClassName(node.outPipeName)
    }
    val extractor = emitJoinFieldList(node)
    val params =
      if (window != null)
        Map("out" -> node.outPipeName,
          "class" -> className,
          "rel1" -> rels.head.name,
          "rel2" -> rels.tail.map(_.name),
          "pairs" -> extractor._1,
          "fields" -> extractor._2,
          "window" -> window._1,
          "wUnit" -> window._2)
      else
        Map("out" -> node.outPipeName,
          "rel1" -> rels.head.name,
          "rel2" -> rels.tail.map(_.name),
          "extractor" -> extractor)
    callST("cross", params)
  }

  /**
   * Generates code for the DISTINCT Operator
   *
   * @param node the DISTINCT operator node
   * @return the Scala code implementing the DISTINCT operator
   */
  def emitDistinct(node: PigOperator): String = {
    callST("distinct", Map("out" -> node.outPipeName, "in" -> node.inPipeName))
  }

  /**
   * Generates code for the FILTER Operator
   *
   * @param pred the filter predicate
   * @param windowMode true if operator is called within a window environment
   * @return the Scala code implementing the FILTER operator
   */
  def emitFilter(node: PigOperator, pred: Predicate, windowMode: Boolean): String = {
    require(node.schema.isDefined)
    val className = schemaClassName(node.schema.get.className)
    val params =
      if (windowMode)
        return ""
      /*Map("out" -> node.outPipeName,
          "in" -> node.inPipeName,
          "pred" -> emitPredicate(node.schema, pred),
          "class" ->className,
          "windowMode" -> windowMode)*/
      else
        Map("out" -> node.outPipeName,
          "in" -> node.inPipeName,
          "class" -> className,
          "pred" -> emitPredicate(CodeGenContext(schema = node.schema), pred))
    callST("filter", params)
  }

  /**
   * Generates code for the FOREACH Operator
   *
   * @param node the FOREACH Operator node
   * @param out name of the output bag
   * @param in name of the input bag
   * @param gen the generate expression
   * @param windowMode true if operator is called within a window environment
   * @return the Scala code implementing the FOREACH operator
   */
  def emitForeach(node: PigOperator, out: String, in: String, gen: ForeachGenerator, windowMode: Boolean): String = {
    // we need to know if the generator contains flatten on tuples or on bags (which require flatMap)
    if (windowMode) return ""
    require(node.schema.isDefined)
    val className = schemaClassName(node.schema.get.className)

    val aggr = !windowMode && containsAggregates(gen)

    val expr = emitForeachExpr(node, gen, aggr)

    val requiresFlatMap = node.asInstanceOf[Foreach].containsFlatten(onBag = true)
    if (aggr) expr
    else if (requiresFlatMap)
      callST("foreachFlatMap", Map("out" -> out, "in" -> in, "expr" -> expr, "class" -> className))
    else
      callST("foreach", Map("out" -> out, "in" -> in, "expr" -> expr, "class" -> className))
  }

  /**
   * Generates code for the GROUPING Operator
   *
   * @param schema the nodes input schema
   * @param out name of the output bag
   * @param in name of the input bag
   * @param groupExpr the grouping expression
   * @param windowMode true if operator is called within a window environment
   * @return the Scala code implementing the GROUPING operator
   */
  def emitGrouping(node: PigOperator, groupExpr: GroupingExpression, windowMode: Boolean): String = {
    if (windowMode) return ""
    require(node.schema.isDefined)
    val className = schemaClassName(node.schema.get.className)
    val out = node.outPipeName
    val in = node.inPipeName
    if (groupExpr.keyList.isEmpty)
      callST("groupBy", Map("out" -> out, "in" -> in, "class" -> className))
    else
      callST("groupBy", Map("out" -> out, "in" -> in,
        "expr" -> emitGroupExpr(CodeGenContext(schema = node.inputSchema), groupExpr), "class" -> className))
  }

  /**
   * Generates code for the JOIN operator.
   *
   * @param node the Join Operator node
   * @param out name of the output bag
   * @param rels list of Pipes to join
   * @param exprs list of join keys
   * @param window window information for Joins on streams
   * @return the Scala code implementing the JOIN operator
   */
  def emitJoin(node: PigOperator, out: String, rels: List[Pipe], exprs: List[List[Ref]], window: Tuple2[Int, String]): String = {
    val className = node.schema match {
      case Some(s) => schemaClassName(s.className)
      case None    => schemaClassName(node.outPipeName)
    }
    val res = node.inputs.zip(exprs)
    val keys = res.map { case (i, k) => emitJoinKey(CodeGenContext(schema = i.producer.schema), k) }

    val extractor = emitJoinFieldList(node)

    // for more than 2 relations the key is nested and needs to be extracted
    var keyLoopPairs = List("t")
    for (k <- keys.drop(2).indices) {
      var nesting = "(t, m0)"
      for (i <- 1 to k) nesting = s"($nesting,m$i)"
      keyLoopPairs :+= nesting
    }
    if (window != null)
      callST("join", Map(
        "out" -> out,
        "class" -> className,
        "rel1" -> rels.head.name,
        "key1" -> keys.head,
        "rel2" -> rels.tail.map(_.name),
        "key2" -> keys.tail,
        "kloop" -> keyLoopPairs,
        "pairs" -> extractor._1,
        "fields" -> extractor._2,
        "window" -> window._1,
        "wUnit" -> window._2.toLowerCase()))
    else
      callST("join", Map(
        "out" -> out,
        "class" -> className,
        "rel1" -> rels.head.name,
        "key1" -> keys.head,
        "rel2" -> rels.tail.map(_.name),
        "key2" -> keys.tail,
        "kloop" -> keyLoopPairs,
        "pairs" -> extractor._1,
        "fields" -> extractor._2))
  }

  /**
   * Generates code for the ORDERBY Operator
   *
   * @param node the OrderBy Operator node
   * @param spec Order specification
   * @param windowMode true if operator is called within a window environment
   * @return the Scala code implementing the ORDERBY operator
   */
  def emitOrderBy(node: PigOperator, spec: List[OrderBySpec], windowMode: Boolean): String = {
    val key = emitSortKey(CodeGenContext(schema = node.schema), spec, node.outPipeName, node.inPipeName)
    val asc = ascendingSortOrder(spec.head)
    callST("orderBy", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "key" -> key, "asc" -> asc))
  }

  /**
   * Generates code for the SOCKET_READ Operator
   *
   * @param node the SOCKET_READ operator
   * @param addr the socket address to connect to
   * @param mode the connection mode, e.g. zmq or empty for standard sockets
   * @param streamFunc an optional stream function (we assume a corresponding Scala function is available)
   * @param streamParams an optional list of parameters to a stream function (e.g. separators)
   * @return the Scala code implementing the SOCKET_READ operator
   */
  def emitSocketRead(node: PigOperator, addr: SocketAddress, mode: String, streamFunc: Option[String], streamParams: List[String]): String = {
    var paramMap = super.emitExtractorFunc(node, streamFunc)
    node.schema match {
      case Some(s) => paramMap += ("class" -> schemaClassName(s.className))
      case None    => paramMap += ("class" -> "Record")
    }
    val params = if (streamParams != null && streamParams.nonEmpty) ", " + streamParams.mkString(",") else ""
    val func = streamFunc.getOrElse(BackendManager.backend.defaultConnector)
    paramMap ++= Map(
      "out" -> node.outPipeName,
      "addr" -> addr,
      "func" -> func,
      "params" -> params)
    if (mode != "") paramMap += ("mode" -> mode)
    callST("socketRead", paramMap)
  }

  /**
   * Generates code for the SOCKET_WRITE Operator
   *
   * @param node the SOCKET_WRITE operator
   * @param addr the socket address to connect to
   * @param mode the connection mode, e.g. zmq or empty for standard sockets
   * @param streamFunc an optional stream function (we assume a corresponding Scala function is available)
   * @param streamParams an optional list of parameters to a stream function (e.g. separators)
   * @return the Scala code implementing the SOCKET_WRITE operator
   */
  def emitSocketWrite(node: PigOperator, addr: SocketAddress, mode: String, streamFunc: Option[String], streamParams: List[String]): String = {
    var paramMap = Map("in" -> node.inPipeName, "addr" -> addr,
      "func" -> streamFunc.getOrElse(BackendManager.backend.defaultConnector))
    node.schema match {
      case Some(s) => paramMap += ("class" -> schemaClassName(s.className))
      case None    => paramMap += ("class" -> "Record")
    }
    if (mode != "") paramMap += ("mode" -> mode)
    if (streamParams != null && streamParams.nonEmpty) paramMap += ("params" -> streamParams.mkString(","))
    callST("socketWrite", paramMap)
  }

  /**
   * Generates code for the WINDOW Operator
   *
   * @param node Window operator
   * @param window window size information (Num, Unit)
   * @param slide window slider information (Num, Unit)
   * @return the Scala code implementing the WINDOW operator
   */
  def emitWindow(node: PigOperator, window: Tuple2[Int, String], slide: Tuple2[Int, String]): String = {
    val isKeyed = node.inputs.head.producer.isInstanceOf[Grouping]
    val isTumbling = window == slide
    val windowIsTime = window._2 != ""
    val slideIsTime = slide._2 != ""
    var paramMap = Map("out" -> node.outPipeName, "in" -> node.inPipeName, "window" -> window._1)

    (isKeyed, isTumbling, windowIsTime, slideIsTime) match {
      // For Keyed Streams
      case (true, true, true, _)        => callST("tumblingTimeWindow", paramMap ++ Map("wUnit" -> window._2.toUpperCase()))
      case (true, true, false, _)       => callST("tumblingCountWindow", paramMap)
      case (true, false, true, true)    => callST("slidingTimeWindow", paramMap ++ Map("wUnit" -> window._2.toUpperCase(), "slider" -> slide._1, "sUnit" -> slide._2.toUpperCase()))
      case (true, false, true, false)   => callST("slidingTimeCountWindow", paramMap ++ Map("wUnit" -> window._2.toUpperCase(), "slider" -> slide._1))
      case (true, false, false, true)   => callST("slidingCountTimeWindow", paramMap ++ Map("slider" -> slide._1, "sUnit" -> slide._2.toUpperCase()))
      case (true, false, false, false)  => callST("slidingCountWindow", paramMap ++ Map("slider" -> slide._1))

      // For Unkeyed Streams
      case (false, true, true, _)       => callST("tumblingTimeWindow", paramMap ++ Map("unkeyed" -> true, "wUnit" -> window._2.toUpperCase()))
      case (false, true, false, _)      => callST("tumblingCountWindow", paramMap ++ Map("unkeyed" -> true))
      case (false, false, true, true)   => callST("slidingTimeWindow", paramMap ++ Map("unkeyed" -> true, "wUnit" -> window._2.toUpperCase(), "slider" -> slide._1, "sUnit" -> slide._2.toUpperCase()))
      case (false, false, true, false)  => callST("slidingTimeCountWindow", paramMap ++ Map("unkeyed" -> true, "wUnit" -> window._2.toUpperCase(), "slider" -> slide._1))
      case (false, false, false, true)  => callST("slidingCountTimeWindow", paramMap ++ Map("unkeyed" -> true, "slider" -> slide._1, "sUnit" -> slide._2.toUpperCase()))
      case (false, false, false, false) => callST("slidingCountWindow", paramMap ++ Map("unkeyed" -> true, "slider" -> slide._1))

      case _                            => ???
    }
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
      case Cross(_, rels, window) => emitCross(node, window)
      case Distinct(_, _, _) => emitDistinct(node)
      case Filter(_, _, pred, windowMode) => emitFilter(node, pred, windowMode)
      case Foreach(_, _, gen, windowMode) => emitForeach(node, node.outPipeName, node.inPipeName, gen, windowMode)
      case Accumulate(out, in, gen) => emitAccumulate(node, gen)
      case Grouping(_, _, groupExpr, windowMode) => emitGrouping(node, groupExpr, windowMode)
      case Join(_, rels, exprs, window) => emitJoin(node, node.outPipeName, node.inputs, exprs, window)
      case OrderBy(_, _, orderSpec, windowMode) => emitOrderBy(node, orderSpec, windowMode)
      case SocketRead(_, address, mode, schema, func, params) => emitSocketRead(node, address, mode, func, params)
      case SocketWrite(_, address, mode, func, params) => emitSocketWrite(node, address, mode, func, params)
      case Window(_, _, window, slide) => emitWindow(node, window, slide)
      case WindowApply(_, _, fname) => callST("windowApply", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "func" -> fname))
      case _ => super.emitNode(node)
    }
  }

}

class FlinkStreamingGenerator(templateFile: String) extends CodeGenerator {
  override val codeGen = new FlinkStreamingCodeGen(templateFile)
}
