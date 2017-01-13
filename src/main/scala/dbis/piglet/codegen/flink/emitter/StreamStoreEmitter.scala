package dbis.piglet.codegen.flink.emitter

class StreamStoreEmitter extends dbis.piglet.codegen.scala_lang.StoreEmitter {
  override def template: String = """    <func>[<class>]().writeStream("<file>", <in><if (params)>, <params><endif>)""".stripMargin
}