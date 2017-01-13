package dbis.piglet.codegen.spark

import dbis.piglet.codegen.scala_lang.LoadEmitter

/**
  * Created by kai on 12.12.16.
  */
class StreamLoadEmitter extends LoadEmitter {
  override def template: String = """    val <out> = <func>[<class>]().loadStream(ssc, "<file>", <extractor><if (params)>, <params><endif>)""".stripMargin
}