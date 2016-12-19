package dbis.piglet.codegen.scala_lang

import dbis.piglet.backends.BackendManager
import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{Load, PigOperator}
import dbis.piglet.schema.Schema

/**
  * Created by kai on 03.12.16.
  */
class LoadEmitter extends CodeEmitter[Load] {
  override def template: String =
    """    val <out> =
      |      <func>[<class>]().load(sc, "<file>", <extractor><if (params)>, <params><endif>)""".stripMargin

  override def code(ctx: CodeGenContext, op: Load): String = {
    var paramMap = ScalaEmitter.emitExtractorFunc(op, op.loaderFunc)
    paramMap += ("out" -> op.outPipeName)
    paramMap += ("file" -> op.file.toString)
    if (op.loaderFunc.isEmpty)
      paramMap += ("func" -> BackendManager.backend.defaultConnector)
    else {
      paramMap += ("func" -> op.loaderFunc.get)
      if (op.loaderParams != null && op.loaderParams.nonEmpty)
        paramMap += ("params" -> op.loaderParams.mkString(","))
    }
    render(paramMap)
  }

}
