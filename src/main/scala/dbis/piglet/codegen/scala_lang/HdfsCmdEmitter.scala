package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.PigOperator
import dbis.piglet.op.cmd.HdfsCmd

/**
  * Created by kai on 12.12.16.
  */
class HdfsCmdEmitter extends CodeEmitter {
  override def template: String = """HDFSService.process("<cmd>", <params>)""".stripMargin

  override def code(ctx: CodeGenContext, node: PigOperator): String = {
    node match {
      case hnode@HdfsCmd(cmd, params) => render(Map("cmd" -> cmd, "params" -> s"List(${hnode.paramString})"))
      case _ => throw CodeGenException(s"unexpected operator: $node")
    }
  }
}
