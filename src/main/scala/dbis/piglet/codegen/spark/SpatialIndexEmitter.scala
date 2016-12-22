package dbis.piglet.codegen.spark

import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.codegen.scala_lang.ScalaEmitter
import dbis.piglet.op.IndexOp
import dbis.piglet.op.IndexMethod
import dbis.piglet.expr.NamedField
import dbis.piglet.expr.PositionalField

class SpatialIndexEmitter extends CodeEmitter[IndexOp] {
  override def template = "val <out> = <in><keyby>.index(<params>)"
  
  override def code(ctx: CodeGenContext, op: IndexOp): String = render(Map(
      "out" -> op.outPipeName,
      "in" -> op.inPipeName,
      "method" -> IndexMethod.methodName(op.method),
      "params" -> op.params.mkString(","),
      "keyby" -> {if(SpatialEmitterHelper.geomIsFirstPos(op.field, op)) "" 
                  else s".keyBy(${ctx.asString("tuplePrefix")} => ${ScalaEmitter.emitRef(CodeGenContext(ctx,Map("schema"->op.inputSchema)), op.field)})"} 
    ) )
}