package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.Dump

/**
  * Created by kai on 05.12.16.
  */
class DumpEmitter extends CodeEmitter[Dump] {
  override def template: String = """ <in>.collect.foreach(t => println(t.mkString()))""".stripMargin


  override def code(ctx: CodeGenContext, op: Dump): String = render(Map("in" -> op.inPipeName))

}
