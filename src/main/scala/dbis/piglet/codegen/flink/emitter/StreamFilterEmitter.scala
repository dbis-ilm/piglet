package dbis.piglet.codegen.flink.emitter

import dbis.piglet.codegen.{ CodeEmitter, CodeGenContext, CodeGenException }
import dbis.piglet.op.Filter
import dbis.piglet.codegen.scala_lang.ScalaEmitter

class StreamFilterEmitter extends CodeEmitter[Filter] {
  override def template: String = """<if (windowMode)>
                                    |    val <out> = <in>.mapWindow(custom<out>Filter _)
                                    |<else>
                                    |    val <out> = <in>.filter(t => {<pred>})
                                    |<endif>
                                    |""".stripMargin
  def templateHelper: String = """    .filter(t => {<pred>})""".stripMargin

  def windowApply(ctx: CodeGenContext, op: Filter): String = {
    CodeEmitter.render(templateHelper, Map("pred" -> ScalaEmitter.emitPredicate(CodeGenContext(ctx, Map("schema" -> op.schema)), op.pred)))
  }
  
  override def code(ctx: CodeGenContext, op: Filter): String = {
    if (op.windowMode) return ""
    if (!op.schema.isDefined)
      throw CodeGenException("FILTER requires a schema definition")

    val className = ScalaEmitter.schemaClassName(op.schema.get.className)
    render(Map("out" -> op.outPipeName,
      "in" -> op.inPipeName,
      "class" -> className,
      "pred" -> ScalaEmitter.emitPredicate(CodeGenContext(ctx, Map[String, Any]("schema" -> op.schema)), op.pred)))
  }
}