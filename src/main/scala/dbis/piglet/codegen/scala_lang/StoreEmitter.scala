package dbis.piglet.codegen.scala_lang

import dbis.piglet.backends.BackendManager
import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{PigOperator, Store}

/**
  * Created by kai on 05.12.16.
  */
class StoreEmitter extends CodeEmitter[Store] {
  override def template: String =
//    """<func>.write("<file>", <in><if (params)>, <params><endif>)""".stripMargin
    """    <func>[<class>]().write("<file>", <in><if (params)>, <params><endif>)""".stripMargin


  override def code(ctx: CodeGenContext, op: Store): String = {
        var paramMap = Map("in" -> op.inPipeName,
          "file" -> op.file.toString,
          "func" -> op.func.getOrElse(BackendManager.backend.defaultConnector))
        op.schema match {
          case Some(s) =>
            val cName = ScalaEmitter.schemaClassName(s)

            paramMap += ("class" -> cName)
          case None => paramMap += ("class" -> "Record")
        }

        if (op.params != null && op.params.nonEmpty)
          paramMap += ("params" -> op.params.mkString(","))
        render(paramMap)
  }

}

object StoreEmitter {
	lazy val instance = new StoreEmitter
}