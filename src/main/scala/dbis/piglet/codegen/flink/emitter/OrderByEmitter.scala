package dbis.piglet.codegen.flink.emitter

import dbis.piglet.codegen.{ CodeEmitter, CodeGenContext, CodeGenException }
import dbis.piglet.op.{ OrderBy, OrderByDirection, OrderBySpec, PigOperator }
import dbis.piglet.schema.Types
import dbis.piglet.expr.NamedField
import dbis.piglet.expr.PositionalField
import dbis.piglet.schema.Schema
import dbis.piglet.expr.Ref
import dbis.piglet.codegen.flink.FlinkHelper

class OrderByEmitter extends dbis.piglet.codegen.scala_lang.OrderByEmitter {
  override def template: String = """    val <out> = <in>.setParallelism(1)<key,asc:{k, a|.sortPartition(<k>, Order.<a>)}>""".stripMargin

  override def code(ctx: CodeGenContext, op: OrderBy): String = {
    val key = op.orderSpec.map(spec => FlinkHelper.getOrderIndex(op.schema, spec.field))
    val orders = op.orderSpec.map(spec => if (spec.dir == OrderByDirection.AscendingOrder) "ASCENDING" else "DESCENDING")
    render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "key" -> key, "asc" -> orders))
  }
}

object OrderByEmitter {
	lazy val instance = new OrderByEmitter
}