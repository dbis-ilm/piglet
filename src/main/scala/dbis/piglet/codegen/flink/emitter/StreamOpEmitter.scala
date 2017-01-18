package dbis.piglet.codegen.flink.emitter

class StreamOpEmitter extends dbis.piglet.codegen.scala_lang.StreamOpEmitter {
  override def template: String = """    val <in>_helper = <in>.map(t => List(<in_fields>))
                                    |    val <out> = <op>(env, <in>_helper<params>).map(t => <class>(<out_fields>))""".stripMargin

}

object StreamOpEmitter {
	lazy val instance = new StreamOpEmitter
}