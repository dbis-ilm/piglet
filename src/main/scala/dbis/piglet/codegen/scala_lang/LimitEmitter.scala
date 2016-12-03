package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{Limit, PigOperator}

/**
  * Created by kai on 03.12.16.
  */
class LimitEmitter extends CodeEmitter {
  override def template: String = """val <out> = sc.parallelize(<in>.take(<num>))""".stripMargin


  override def code(ctx: CodeGenContext, node: PigOperator): String = {
    node match {
      case Limit(out, in, num) =>
        render(Map("out" -> node.outPipeName, "in" -> node.inPipeName, "num" -> num))
      case _ => throw CodeGenException(s"unexpected operator: $node")
    }
  }

}
