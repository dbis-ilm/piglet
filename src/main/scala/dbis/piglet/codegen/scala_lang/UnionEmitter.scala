package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{Union, PigOperator}

/**
  * Created by kai on 03.12.16.
  */
class UnionEmitter extends CodeEmitter[Union] {
  override def template: String = """val <out> = <in><others:{ e | .union(<e>)}>""".stripMargin


  override def code(ctx: CodeGenContext, op: Union): String = render(Map("out" -> op.outPipeName,
          "in" -> op.inPipeName,
          "others" -> op.inPipeNames.tail))

}

object UnionEmitter {
	lazy val instance = new UnionEmitter
}