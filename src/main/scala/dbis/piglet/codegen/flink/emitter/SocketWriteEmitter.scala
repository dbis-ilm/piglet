package dbis.piglet.codegen.flink.emitter

import dbis.piglet.op.SocketWrite
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.backends.BackendManager
import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.codegen.scala_lang.ScalaEmitter



class SocketWriteEmitter extends CodeEmitter[SocketWrite] {
  override def template: String = """<if (mode)>
                                    |    <func>[<class>]().zmqPublish("<addr.protocol><addr.hostname>:<addr.port>", <in><if (params)>, <params><endif>)
                                    |<else>
                                    |    <func>[<class>]().bind("<addr.hostname>", <addr.port>, <in><if (params)>, <params><endif>)
                                    |<endif>""".stripMargin

  override def code(ctx: CodeGenContext, op: SocketWrite): String = {
    var paramMap = Map("in" -> op.inPipeName, "addr" -> op.addr,
      "func" -> op.func.getOrElse(BackendManager.backend.defaultConnector))
    op.schema match {
      case Some(s) => paramMap += ("class" -> ScalaEmitter.schemaClassName(s.className))
      case None => paramMap += ("class" -> "Record")
    }
    if (op.mode != "") paramMap += ("mode" -> op.mode)
    if (op.params != null && op.params.nonEmpty) paramMap += ("params" -> op.params.mkString(","))
    render(paramMap)
  }
}