package dbis.piglet.codegen.flink.emitter

import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.op.SocketRead
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.backends.BackendManager
import dbis.piglet.codegen.scala_lang.ScalaEmitter

class SocketReadEmitter extends CodeEmitter[SocketRead] {
  override def template: String = """<if (mode)>
                                    |    val <out> = <func>[<class>]().zmqSubscribe(env, "<addr.protocol><addr.hostname>:<addr.port>", <extractor><params>)
                                    |<else>
                                    |    val <out> = <func>[<class>]().connect(env, "<addr.hostname>", <addr.port>, <extractor><params>)    
                                    |<endif>""".stripMargin

  override def code(ctx: CodeGenContext, op: SocketRead): String = {
    var paramMap = ScalaEmitter.emitExtractorFunc(op, op.streamFunc)
    op.schema match {
      case Some(s) => paramMap += ("class" -> ScalaEmitter.schemaClassName(s.className))
      case None => paramMap += ("class" -> "Record")
    }
    val params = if (op.streamParams != null && op.streamParams.nonEmpty) ", " + op.streamParams.mkString(",") else ""
    val func = op.streamFunc.getOrElse(BackendManager.backend.defaultConnector)
    paramMap ++= Map(
      "out" -> op.outPipeName,
      "addr" -> op.addr,
      "func" -> func,
      "params" -> params)
    if (op.mode != "") paramMap += ("mode" -> op.mode)
    render(paramMap)
  }
}