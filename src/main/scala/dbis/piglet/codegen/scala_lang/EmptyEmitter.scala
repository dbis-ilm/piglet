package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{Empty, PigOperator}

/**
  * Created by kai on 09.12.16.
  */
class EmptyEmitter extends CodeEmitter {
  override def template: String = ""

  override def code(ctx: CodeGenContext, node: PigOperator): String = {

    node match {
      case Empty(_) => ""
      case _ => throw CodeGenException(s"unexpected operator: $node")
    }
  }
}
