package dbis.piglet.codegen.flink.emitter

class StreamLoadEmitter extends dbis.piglet.codegen.scala_lang.LoadEmitter {
  override def template: String = """    val <out> = <func>[<class>]().loadStream(env, "<file>", <extractor><if (params)>, <params><endif>)""".stripMargin
}