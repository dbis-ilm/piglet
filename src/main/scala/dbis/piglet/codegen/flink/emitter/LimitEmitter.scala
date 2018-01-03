package dbis.piglet.codegen.flink.emitter

import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.op.Limit

class LimitEmitter extends dbis.piglet.codegen.scala_lang.LimitEmitter {
  override def template: String = """    val <out> = <in>.first(<num>)""".stripMargin

  override def code(ctx: CodeGenContext, op: Limit): String = {

    val params = Map(
      "out" -> op.outPipeName,
      "in" -> op.inPipeName,
      "num" -> op.num,
      "lineage" -> op.lineageSignature)

    render(params)

  }

}

object LimitEmitter {
	lazy val instance = new LimitEmitter
}