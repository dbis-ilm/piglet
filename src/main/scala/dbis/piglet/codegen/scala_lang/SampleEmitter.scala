package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{Sample, PigOperator}

/**
  * Created by kai on 03.12.16.
  */
class SampleEmitter extends CodeEmitter[Sample] {
  override def template: String = """ val <out> = <in>.sample(false, <expr>)""".stripMargin


  override def code(ctx: CodeGenContext, op: Sample): String = render(Map("out" -> op.outPipeName,
          "in" -> op.inPipeName,
          "expr" -> ScalaEmitter.emitExpr(ctx, op.expr)))

}


object SampleEmitter {
	lazy val instance = new SampleEmitter
}