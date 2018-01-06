package dbis.piglet.codegen.scala_lang

import dbis.piglet.backends.BackendManager
import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.Load

/**
  * Created by kai on 03.12.16.
  */
class LoadEmitter extends CodeEmitter[Load] {
  override def template: String =
//    """val <out> = <func>.load[<class>](sc,"<file>"<if (extractor)>, <extractor><endif><if (params)>, <params><endif>)"""
    """    val <out> = <func>[<class>](<if (profiling)>randFactor<endif>).load(sc, "<file>"<if (extractor)>, <extractor><endif><if (params)>, <params><endif>, <if (profiling)>lineageAndAccum = Some(("<lineage>",accum))<else>lineageAndAccum = None<endif>)""".stripMargin



  override def code(ctx: CodeGenContext, op: Load): String = {
    var paramMap = ScalaEmitter.emitExtractorFunc(op, op.loaderFunc)
    paramMap += ("out" -> op.outPipeName)
    paramMap += ("file" -> op.file.toString)
    paramMap += ("lineage" -> op.lineageSignature)
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

object LoadEmitter {
  lazy val instance = new LoadEmitter
}
