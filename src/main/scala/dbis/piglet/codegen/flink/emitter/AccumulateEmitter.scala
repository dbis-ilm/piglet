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
import scala.collection.mutable.ListBuffer
import dbis.piglet.codegen.CodeGenException

class AccumulateEmitter extends dbis.piglet.codegen.scala_lang.AccumulateEmitter {
  override def template: String = """    val <out> = <in>.aggregate(Aggregations.<init_aggr_expr>)<more_aggr_expr:{exp|.and(Aggregations.<exp>)}>""".stripMargin

  override def code(ctx: CodeGenContext, op: Accumulate): String = {
    if (!op.schema.isDefined)
      throw CodeGenException("schema required in ACCUMULATE")
    val inputSchemaDefined = op.inputSchema.isDefined
    val outClassName = ScalaEmitter.schemaClassName(op.schema.get.className)
    var initAggrFun: String = ""
    var moreAggrFuns: ListBuffer[String] = new ListBuffer()
    val updExpr = op.generator.exprs.zipWithIndex.map {
      case (e, i) =>
        require(e.expr.isInstanceOf[Func])
        val funcName = e.expr.asInstanceOf[Func].f.toUpperCase

        val traverse = new RefExprExtractor
        e.expr.traverseAnd(null, traverse.collectRefExprs)
        val refExpr = traverse.exprs.head

        val str: String = refExpr.r match {
          case nf @ NamedField(n, _) => s"$op.inputSchema.get.indexOfField(nf)"
          case PositionalField(p) => if (inputSchemaDefined) s"$p" else "0"
          case _ => ""
        }
        if (i == 0) initAggrFun = (funcName + "," + str) else moreAggrFuns += (funcName + "," + str)
    }

    render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "class" -> outClassName, 
        "init_aggr_expr" -> initAggrFun, "more_aggr_expr" -> moreAggrFuns))
  }
}