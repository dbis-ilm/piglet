package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.PigOperator
import dbis.piglet.op.cmd.HdfsCmd

/**
  * Created by kai on 12.12.16.
  */
class HdfsCmdEmitter extends CodeEmitter[HdfsCmd] {
  override def template: String = """HDFSService.process("<cmd>", <params>)""".stripMargin

  override def code(ctx: CodeGenContext, op: HdfsCmd): String = render(Map("cmd" -> op.cmd, "params" -> s"List(${op.paramString})"))
}

object HdfsCmdEmitter {
  lazy val instance = new HdfsCmdEmitter
}