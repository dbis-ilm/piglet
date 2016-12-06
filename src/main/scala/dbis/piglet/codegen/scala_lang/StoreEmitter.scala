package dbis.piglet.codegen.scala_lang

import dbis.piglet.backends.BackendManager
import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{PigOperator, Store}

/**
  * Created by kai on 05.12.16.
  */
class StoreEmitter extends CodeEmitter {
  override def template: String = """<func>[<class>]().write("<file>", <in><if (params)>, <params><endif>)""".stripMargin


  override def code(ctx: CodeGenContext, node: PigOperator): String = {
    node match {
      case Store(in, file, storeFunc, params) => {
        var paramMap = Map("in" -> node.inPipeName,
          "file" -> file.toString,
          "func" -> storeFunc.getOrElse(BackendManager.backend.defaultConnector))
        node.schema match {
          case Some(s) => paramMap += ("class" -> ScalaEmitter.schemaClassName(s.className))
          case None => paramMap += ("class" -> "Record")
        }

        if (params != null && params.nonEmpty)
          paramMap += ("params" -> params.mkString(","))
        render(paramMap)
      }
      case _ => throw CodeGenException(s"unexpected operator: $node")
    }
  }

}
