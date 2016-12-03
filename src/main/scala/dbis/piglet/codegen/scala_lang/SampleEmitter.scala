package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{Sample, PigOperator}

/**
  * Created by kai on 03.12.16.
  */
class SampleEmitter extends CodeEmitter {
  override def template: String = """ val <out> = <in>.sample(false, <expr>)""".stripMargin


  override def code(ctx: CodeGenContext, node: PigOperator): String = {
    node match {
      case Sample(out, in, expr) =>
        render(Map("out" -> node.outPipeName,
          "in" -> node.inPipeName,
          "expr" -> ScalaEmitter.emitExpr(ctx, expr)))
      case _ => throw CodeGenException(s"unexpected operator: $node")
    }
  }

}
