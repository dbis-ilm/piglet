package dbis.piglet.codegen.flink.emitter

class StreamDumpEmitter extends dbis.piglet.codegen.scala_lang.DumpEmitter {
  override def template: String = """    <in>.map(_.mkString()).print""".stripMargin
}