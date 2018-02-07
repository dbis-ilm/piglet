package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.Difference

class DifferenceEmitter extends CodeEmitter[Difference] {
  override def template: String = """val <out> = <in1>.subtract(<in2>)""".stripMargin


  override def code(ctx: CodeGenContext, op: Difference): String = render(Map("out" -> op.outPipeName,
    "in1" -> op.inPipeNames.head,
    "in2" -> op.inPipeNames.last
  ))

}

object DifferenceEmitter {
  lazy val instance = new DifferenceEmitter
}