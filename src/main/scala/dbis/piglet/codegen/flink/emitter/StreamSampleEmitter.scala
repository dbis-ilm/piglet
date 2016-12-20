package dbis.piglet.codegen.flink.emitter

class StreamSampleEmitter extends dbis.piglet.codegen.scala_lang.SampleEmitter {
  override def template: String = """    val <out> = <in>.filter(t => util.Random.nextDouble \<= <expr>)""".stripMargin
}