package dbis.piglet.codegen.flink.emitter

class LoadEmitter extends dbis.piglet.codegen.scala_lang.LoadEmitter {
  override def template: String = """val <out> = <func>[<class>]().load(env, "<file>", <extractor><if (params)>, <params><endif>)""".stripMargin

}

object LoadEmitter {
	lazy val instance = new LoadEmitter
}