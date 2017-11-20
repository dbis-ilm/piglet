package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.Limit

/**
  * Created by kai on 03.12.16.
  */
class LimitEmitter extends CodeEmitter[Limit] {
  // val <out> = sc.parallelize(<in>.take(<num>))
  override def template: String =
    """val <out> = <in>.zipWithIndex.filter{case (_,idx) => idx \< <num>}.map{t =>
      |  val res = t._1
      |  <if (profiling)>
      |  if(scala.util.Random.nextInt(randFactor) == 0) {
      |    PerfMonitor.sampleSize(res,"<lineage>", accum)
      |  }
      |  <endif>
      |  res
      |}""".stripMargin


  override def code(ctx: CodeGenContext, op: Limit): String = 
        render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "num" -> op.num,"lineage" -> op.lineageSignature))

}

object LimitEmitter {
  lazy val instance = new LimitEmitter
}