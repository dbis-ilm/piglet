package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.expr._
import dbis.piglet.op.{Accumulate, PigOperator}
import dbis.piglet.schema.Types

import scala.collection.mutable

/**
  * Created by kai on 12.12.16.
  */
class AccumulateEmitter extends CodeEmitter[Accumulate] {
  override def template: String = """val <out>_fold = <in>.map(t => <helper_class>(t, <init_expr>))
                                    |                                        .aggregate(<helper_class>())(aggr_<out>_seq, aggr_<out>_comp)
                                    |        val <out> = sc.parallelize(Array(<class>(<aggr_expr>)))""".stripMargin

  def aggrTemplate: String = """def aggr_<out>_seq(acc: <helper_class>, v: <helper_class>): <helper_class> =
                               |                <helper_class>(v._t, <seq_expr>)
                               |        def aggr_<out>_comp(acc: <helper_class>, v: <helper_class>): <helper_class> =
                               |                <helper_class>(v._t, <comp_expr>)""".stripMargin

  def classTemplate: String = """case class <name> (<fields>) extends java.io.Serializable with SchemaClass {
                                |  override def mkString(_c: String = ",") = <string_rep>
                                |}""".stripMargin

  private def callsAverageFunc(op: PigOperator, e: Expr): Boolean =
    e.traverseOr(op.inputSchema.getOrElse(null), Expr.containsAverageFunc)

  private def callsCountFunc(op: PigOperator, e: Expr): Boolean =
    e.traverseOr(op.inputSchema.getOrElse(null), Expr.containsCountFunc)

  override def helper(ctx: CodeGenContext, op: Accumulate): String = {

    if(AccumulateEmitter.helperClasses.contains(op.schema.get.className)) {
      return ""
    } else {
      AccumulateEmitter.helperClasses += op.schema.get.className
    }

    // TODO: for ACCUMULATE we need a special tuple class
    val inSchemaClassName = op.inputSchema match {
      case Some(s) => ScalaEmitter.schemaClassName(s.className)
      case None => "Record"
    }
    val fieldStr = s"_t: ${inSchemaClassName} = null, " + op.generator.exprs.zipWithIndex.map{ case (e, i) =>
      if (callsAverageFunc(op, e.expr)) {
        // TODO: determine type
        val inType = "Long"
        s"_${i}sum: ${inType} = 0, _${i}cnt: Long = 0"
      }
      else {
        val resType = e.expr.resultType(op.inputSchema)
        require(e.expr.isInstanceOf[Func])
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
    CodeEmitter.render(classTemplate, Map("name" -> s"_${op.schema.get.className}_HelperTuple",
      "fields" -> fieldStr,
      "string_rep" -> "\"\""))
  }

  override def code(ctx: CodeGenContext, op: Accumulate): String = {
    val inputSchemaDefined = op.inputSchema.isDefined
    require(op.schema.isDefined)
    val outClassName = ScalaEmitter.schemaClassName(op.schema.get.className)
    val helperClassName = s"_${op.schema.get.className}_HelperTuple"

    // generate update expression
    val updExpr = op.generator.exprs.zipWithIndex.map { case (e, i) =>
      // AVG requires special handling
      if (callsAverageFunc(op, e.expr))
        s"PigFuncs.incrSUM(acc._${i}sum, v._${i}sum), PigFuncs.incrCOUNT(acc._${i}cnt, v._${i}cnt)"
      else {
        require(e.expr.isInstanceOf[Func])
        val func = e.expr.asInstanceOf[Func]
        val funcName = func.f.toUpperCase
        s"PigFuncs.incr${funcName}(acc._$i, v._$i)"
      }
    }.mkString(", ")
    // generate final combination expression
    val compExpr = op.generator.exprs.zipWithIndex.map { case (e, i) =>
      // AVG requires special handling
      if (callsAverageFunc(op, e.expr))
        s"PigFuncs.incrSUM(acc._${i}sum, v._${i}sum), PigFuncs.incrSUM(acc._${i}cnt, v._${i}cnt)"
      else {
        require(e.expr.isInstanceOf[Func])
        val func = e.expr.asInstanceOf[Func]
        var funcName = func.f.toUpperCase
        if (funcName == "COUNT") funcName = "SUM"
        s"PigFuncs.incr${funcName}(acc._$i, v._$i)"
      }
    }.mkString(", ")

    // generate init expression
    val initExpr = op.generator.exprs.map { e =>
      // For each expression in GENERATE we have to extract the referenced fields.
      // So, we extract all RefExprs.
      val traverse = new RefExprExtractor
      e.expr.traverseAnd(null, traverse.collectRefExprs)
      val refExpr = traverse.exprs.head
      // in case of COUNT we simply pass 0 instead of the field: this allows COUNT on chararray
      if (callsCountFunc(op, e.expr))
        "0"
      else {
        val str = refExpr.r match {
          case nf@NamedField(n, _) => s"t._${op.inputSchema.get.indexOfField(nf)}"
          case PositionalField(p) => if (inputSchemaDefined) s"t._$p" else s"t.get(0)"
          case _ => ""
        }
        // in case of AVERAGE we need fields for SUM and COUNT
        if (callsAverageFunc(op, e.expr)) s"${str}, ${str}" else str
      }
    }.mkString(", ")

    // generate final aggregation expression
    val aggrExpr = op.generator.exprs.zipWithIndex.map { case (e, i) =>
      // For each expression we need to know the corresponding field in the helper tuple.
      // But because we assume that the expressions are only aggregate functions, we have to
      // distinguish only between avg and other functions.
      if (callsAverageFunc(op, e.expr))
        s"${op.outPipeName}_fold._${i}sum.toDouble / ${op.outPipeName}_fold._${i}cnt.toDouble"
      else
        s"${op.outPipeName}_fold._$i"
    }.mkString(", ")

    var res = CodeEmitter.render(aggrTemplate, Map("out" -> op.outPipeName,
      "helper_class" -> helperClassName,
      "seq_expr" -> updExpr,
      "comp_expr" -> compExpr))
    res += "\n"
    res += render(Map("out" -> op.outPipeName,
      "in" -> op.inPipeName,
      "helper_class" -> helperClassName,
      "class" -> outClassName,
      "init_expr" -> initExpr,
      "aggr_expr" -> aggrExpr))
    res
  }
}

object AccumulateEmitter {

  val helperClasses = mutable.Set.empty[String]

  lazy val instance = new AccumulateEmitter
}