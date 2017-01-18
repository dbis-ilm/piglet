package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.Empty

/**
  * Created by kai on 09.12.16.
  */
class EmptyEmitter extends CodeEmitter[Empty] {
  override def template: String = ""

  override def code(ctx: CodeGenContext, node: Empty): String = template
}

object EmptyEmitter {
  lazy val instance = new EmptyEmitter
}