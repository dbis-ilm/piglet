package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{Distinct, PigOperator}

/**
  * Created by kai on 03.12.16.
  */
class DistinctEmitter extends CodeEmitter {
  override def template: String = """val <out> = <in>.distinct""".stripMargin


  override def code(ctx: CodeGenContext, node: PigOperator): String = {
    node match {
      case Distinct(out, in, _) =>
        render(Map("out" -> node.outPipeName, "in" -> node.inPipeName))
      case _ => throw CodeGenException(s"unexpected operator: $node")
    }
  }

}
