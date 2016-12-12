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


  /**
    * Construct the extract function for the LOAD operator.
    *
    * @param node the PigOperator for loading data
    * @param loaderFunc the loader function
    * @return a parameter map with class and extractor elements
    */
  def emitExtractorFunc(node: PigOperator, loaderFunc: Option[String]): Map[String, Any] = {
    def schemaExtractor(schema: Schema): String =
      schema.fields.zipWithIndex.map{case (f, i) =>
        // we cannot perform a "toAny" - therefore, we treat bytearray as String here
        val t = ScalaEmitter.scalaTypeMappingTable(f.fType); s"data($i).to${if (t == "Any") "String" else t}"
      }.mkString(", ")

    def jdbcSchemaExtractor(schema: Schema): String =
      schema.fields.zipWithIndex.map{case (f, i) => s"data.get${ScalaEmitter.scalaTypeMappingTable(f.fType)}($i)"}.mkString(", ")

    var paramMap = Map[String, Any]()
    node.schema match {
      case Some(s) => if (loaderFunc.nonEmpty && loaderFunc.get == "JdbcStorage")
      // JdbcStorage provides already types results, therefore we need an extractor which calls
      // only the appropriate get functions on sql.Row
        paramMap += ("extractor" ->
          s"""(data: org.apache.spark.sql.Row) => ${ScalaEmitter.schemaClassName(s.className)}(${jdbcSchemaExtractor(s)})""",
          "class" -> ScalaEmitter.schemaClassName(s.className))
      else
        paramMap += ("extractor" ->
          s"""(data: Array[String]) => ${ScalaEmitter.schemaClassName(s.className)}(${schemaExtractor(s)})""",
          "class" -> ScalaEmitter.schemaClassName(s.className))
      case None => {
        paramMap += ("extractor" -> "(data: Array[String]) => Record(data)", "class" -> "Record")
      }
    }
    paramMap
  }


  override def code(ctx: CodeGenContext, node: PigOperator): String = {
    node match {
      case Load(out, file, schema, loaderFunc, loaderParams) => {
        var paramMap = emitExtractorFunc(node, loaderFunc)
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
