package dbis.piglet.codegen.flink.emitter

class LimitEmitter extends dbis.piglet.codegen.scala_lang.LimitEmitter {
  override def template: String = """    val <out> = <in>.first(<num>)""".stripMargin
}

object LimitEmitter {
	lazy val instance = new LimitEmitter
}