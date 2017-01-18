package dbis.piglet.codegen.flink.emitter

import dbis.piglet.expr.Func
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.op.Accumulate
import dbis.piglet.codegen.scala_lang.ScalaEmitter
import dbis.piglet.expr.Expr
import scala.collection.mutable.{ Map => MMap }
import dbis.piglet.expr.DerefTuple
import dbis.piglet.expr.RefExprExtractor
import dbis.piglet.expr.NamedField
import dbis.piglet.expr.PositionalField
import dbis.piglet.schema.Types
import dbis.piglet.op._
import dbis.piglet.expr._
import dbis.piglet.udf._
import dbis.piglet.schema._
import dbis.piglet.codegen.CodeEmitter

class StreamAccumulateEmitter extends dbis.piglet.codegen.scala_lang.AccumulateEmitter {
  override def template: String = """    val <out>_fold = <in><if (notKeyed)>.keyBy(t => 0)<endif>.mapWithState(streamAcc_<out>)
                                    |    val <out> = <out>_fold.map(t => <class>(<aggr_expr>))""".stripMargin
  
  override def aggrTemplate: String  = """  def streamAcc_<out> = (t: <class>, state: Option[<helper_class>]) => {
                                          |    val acc: <helper_class> = state.getOrElse(<helper_class>())
                                          |    val v = <helper_class>(t, <init_expr>)
                                          |    val updatedState = <helper_class>(v._t, <expr>)
                                          |    (updatedState, Some(updatedState))
                                          |  }""".stripMargin

  override def helper(ctx: CodeGenContext, op: Accumulate): String = {
    val inSchemaClassName = op.inputSchema match {
      case Some(s) => ScalaEmitter.schemaClassName(s.className)
      case None => "Record"
    }
    val fieldStr = s"_t: ${inSchemaClassName} = null, " + op.generator.exprs.filter(e => e.expr.isInstanceOf[Func]).zipWithIndex.map {
      case (e, i) =>
        if (e.expr.traverseOr(op.inputSchema.getOrElse(null), Expr.containsAverageFunc)) {
          val inType = "Long"
          s"_${i}sum: ${inType} = 0, _${i}cnt: Long = 0"
        } else {
          val resType = e.expr.resultType(op.inputSchema)
          val funcName = e.expr.asInstanceOf[Func].f.toUpperCase
          val defaultValue = if (Types.isNumericType(resType)) funcName match {
            // for min and max we need special initial values
            case "MIN" => "Int.MaxValue"
            case "MAX" => "Int.MinValue"
            case _ => 0
          }
          else "null"
          s"_$i: ${ScalaEmitter.scalaTypeMappingTable(resType)} = ${defaultValue}"
        }
    }.mkString(", ")
    val schemaHelper = CodeEmitter.render(classTemplate, Map("name" -> s"_${op.schema.get.className}_HelperTuple",
      "fields" -> fieldStr,
      "string_rep" -> "\"\""))

    val expr = op.generator.exprs.filter(e => e.expr.isInstanceOf[Func]).zipWithIndex.map {
      case (e, i) =>
        if (e.expr.traverseOr(op.inputSchema.getOrElse(null), Expr.containsAverageFunc))
          s"PigFuncs.incrSUM(acc._${i}sum, v._${i}sum), PigFuncs.incrCOUNT(acc._${i}cnt, v._${i}cnt)"
        else {
          val func = e.expr.asInstanceOf[Func]
          val funcName = func.f.toUpperCase
          s"PigFuncs.incr${funcName}(acc._$i, v._$i)"
        }
    }.mkString(", ")

    // generate init expression
    val initExpr = op.generator.exprs.filter(e => e.expr.isInstanceOf[Func]).map { e =>
      // For each expression in GENERATE we have to extract the referenced fields.
      // So, we extract all RefExprs.
      val traverse = new RefExprExtractor
      e.expr.traverseAnd(null, traverse.collectRefExprs)
      val refExpr = traverse.exprs.head
      // in case of COUNT we simply pass 0 instead of the field: this allows COUNT on chararray
      if (e.expr.traverseOr(op.inputSchema.getOrElse(null), Expr.containsCountFunc))
        "0"
      else {
        val str = refExpr.r match {
          case nf @ NamedField(n, _) => s"t._${op.inputSchema.get.indexOfField(nf)}"
          case PositionalField(p) => if (op.inputSchema.isDefined) s"t._$p" else s"t.get(0)"
          case DerefTuple(r1, r2) => ScalaEmitter.emitRef(CodeGenContext(ctx, Map("schema" -> op.inputSchema, "tuplePrefix" -> "t")), r1) + ".head" + 
                                     ScalaEmitter.emitRef(CodeGenContext(ctx, Map("schema" -> ScalaEmitter.tupleSchema(op.inputSchema, r1), "tuplePrefix" -> "")), r2)
          case _ => ""
        }
        // in case of AVERAGE we need fields for SUM and COUNT
        if (e.expr.traverseOr(op.inputSchema.getOrElse(null), Expr.containsAverageFunc)) s"${str}, ${str}" else str
      }
    }.mkString(", ")

    val streamFunc = CodeEmitter.render(aggrTemplate, Map("out" -> op.outPipeName,
      "helper_class" -> s"_${op.schema.get.className}_HelperTuple",
      "class" -> ScalaEmitter.schemaClassName(op.inputSchema.get.className),
      "expr" -> expr,
      "init_expr" -> initExpr))
    schemaHelper + "\n" + streamFunc
  }

  override def code(ctx: CodeGenContext, op: Accumulate): String = {
    val inputSchemaDefined = op.inputSchema.isDefined
    require(op.schema.isDefined)
    val outClassName = ScalaEmitter.schemaClassName(op.schema.get.className)
    val helperClassName = s"_${op.schema.get.className}_HelperTuple"
    // Check if input is already keyed/grouped, as it is necessary to use for mapWithState()
    val keyed = inputSchemaDefined && op.inputSchema.get.fields.size == 2 && op.inputSchema.get.field(0).name == "group"

    // generate final aggregation expression
    var aggrCounter = -1
    val aggrExpr = op.generator.exprs.map { e =>
      if (e.expr.isInstanceOf[Func]) {
        aggrCounter += 1
        if (e.expr.traverseOr(op.inputSchema.getOrElse(null), Expr.containsAverageFunc))
          s"t._${aggrCounter}sum.toDouble / t._${aggrCounter}cnt.toDouble"
        else
          s"t._$aggrCounter"
      } else
        "t._t" + s"${ScalaEmitter.emitExpr(CodeGenContext(ctx, Map("schema" -> op.inputSchema)), e.expr)}".drop(1)
    }.mkString(", ")
    val params = MMap[String, Any]()
    params += "out" -> op.outPipeName
    params += "in" -> op.inPipeName
    params += "helper_class" -> helperClassName
    params += "class" -> outClassName
    params += "aggr_expr" -> aggrExpr
    if (!keyed) params += "notKeyed" -> true

    render(params.toMap)
  }
}

object StreamAccumulateEmitter {
	lazy val instance = new StreamAccumulateEmitter
}