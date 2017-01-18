package dbis.piglet.codegen.flink.emitter

class StoreEmitter extends dbis.piglet.codegen.scala_lang.StoreEmitter {
  override def template: String = """    <func>[<class>]().write("<file>", <in><if (params)>, <params><endif>)
                                    |    env.execute("Starting Query")""".stripMargin
}

object StoreEmitter {
	lazy val instance = new StoreEmitter
}