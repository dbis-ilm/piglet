package dbis.piglet.codegen.flink.emitter

class DumpEmitter extends dbis.piglet.codegen.scala_lang.DumpEmitter {
  override def template: String = """    <in>.map(_.mkString()).print""".stripMargin
}

object DumpEmitter {
	lazy val instance = new DumpEmitter
}