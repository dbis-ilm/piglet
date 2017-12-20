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
      |    PerfMonitor.sampleSize(res,"<lineage>", accum, randFactor)
      |  <endif>
      |  res
      |}""".stripMargin

  val smallLimitTemplate = s"""val <out> = sc.parallelize(<in>.take(<num>))<if (profiling)>.map{e =>
    |  PerfMonitor.sampleSize(e,"<lineage>", accum, randFactor)
    |  e
    |}<endif>""".stripMargin

  override def code(ctx: CodeGenContext, op: Limit): String = {

    val params = Map(
      "out" -> op.outPipeName,
      "in" -> op.inPipeName,
      "num" -> op.num,
      "lineage" -> op.lineageSignature)

    if(op.num <= 1000)
      CodeEmitter.render(smallLimitTemplate,params)
    else
      render(params)

  }





}

object LimitEmitter {
  lazy val instance = new LimitEmitter
}