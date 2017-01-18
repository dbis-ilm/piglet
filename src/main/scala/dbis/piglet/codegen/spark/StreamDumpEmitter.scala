package dbis.piglet.codegen.spark

import dbis.piglet.codegen.scala_lang.DumpEmitter

/**
  * Created by kai on 12.12.16.
  */

class StreamDumpEmitter extends DumpEmitter {
  override def template: String = """    <in>.foreachRDD(rdd => rdd.foreach(elem => println(elem.mkString())))""".stripMargin
}

object StreamDumpEmitter {
	lazy val instance = new StreamDumpEmitter
}