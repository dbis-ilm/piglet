package dbis.piglet.codegen.spark

import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.op.SpatialFilter
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.codegen.scala_lang.ScalaEmitter

class SpatialFilterEmitter extends CodeEmitter[SpatialFilter] {
  
  override def template = "val <out> = <in>.keyBy(t => <field>).<predicate>(<other>).map{case (g,v) => v}"
  
  override def code(ctx: CodeGenContext, op: SpatialFilter): String = render(Map("out" -> op.outPipeName,
          "in" -> op.inPipeName,
          "predicate" -> op.pred.predicateType.toString().toLowerCase(),
          "field" -> ScalaEmitter.emitRef(ctx, op.pred.field),
          "other" -> ScalaEmitter.emitExpr(ctx, op.pred.expr)
        ))
  
}