package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.Dump

/**
  * Created by kai on 05.12.16.
  */
class DumpEmitter extends CodeEmitter[Dump] {//
  override def template: String = """<if(mute)><in>.foreach{t=><if (profiling)>
                                    |if(scala.util.Random.nextInt(randFactor) == 0) {
                                    |  accum.incr("<lineage>", t.getNumBytes)
                                    |}<endif>}<else>
                                    |<in><if (profiling)>.map{t =>
                                    |  if(scala.util.Random.nextInt(randFactor) == 0) {
                                    |    accum.incr("<lineage>", t.getNumBytes)
                                    |  }
                                    |  t
                                    |}<endif>.collect.foreach(t => println(t.mkString()))<endif>""".stripMargin


  override def code(ctx: CodeGenContext, op: Dump): String = {
    val map = collection.mutable.Map("in" -> op.inPipeName,  "lineage" -> op.lineageSignature)
    if(op.mute)
      map += ("mute" -> op.mute.toString)
    render(map.toMap)
  }

}

object DumpEmitter {
  lazy val instance = new DumpEmitter 
}
