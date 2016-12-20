package dbis.piglet.codegen.flink.emitter

import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.op.Distinct
import dbis.piglet.codegen.CodeEmitter

class StreamDistinctEmitter extends dbis.piglet.codegen.scala_lang.DistinctEmitter {
  override def template: String = """""".stripMargin
  def templateHelper: String = "		.toList.distinct"
  
  def windowApply(ctx: CodeGenContext, op: Distinct): String = {
    CodeEmitter.render(templateHelper, Map())
  }
}