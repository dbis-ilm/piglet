package dbis.piglet.codegen.scala_lang

import dbis.piglet.backends.BackendManager
import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{Load, PigOperator}
import dbis.piglet.schema.Schema

/**
  * Created by kai on 03.12.16.
  */
class LoadEmitter extends CodeEmitter {
  override def template: String =
    """    val <out> =
      |      <func>[<class>]().load(sc, "<file>", <extractor><if (params)>, <params><endif>)""".stripMargin

  override def code(ctx: CodeGenContext, node: PigOperator): String = {
    node match {
      case Load(out, file, schema, loaderFunc, loaderParams) => {
        var paramMap = ScalaEmitter.emitExtractorFunc(node, loaderFunc)
        paramMap += ("out" -> node.outPipeName)
        paramMap += ("file" -> file.toString)
        if (loaderFunc.isEmpty)
          paramMap += ("func" -> BackendManager.backend.defaultConnector)
        else {
          paramMap += ("func" -> loaderFunc.get)
          if (loaderParams != null && loaderParams.nonEmpty)
            paramMap += ("params" -> loaderParams.mkString(","))
        }
        render(paramMap)
      }
      case _ => throw CodeGenException(s"unexpected operator: $node")
    }
  }

}
