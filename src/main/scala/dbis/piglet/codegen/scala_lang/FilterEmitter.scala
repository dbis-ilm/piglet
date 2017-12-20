package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.Filter

/**
  * Created by kai on 01.12.16.
  */
class FilterEmitter extends CodeEmitter[Filter] {
  override def template: String =
    """val <out> = <in>.filter{t =>
      |  val res = <pred>
      |  <if (profiling)>
      |  if(res) {
      |    PerfMonitor.sampleSize(t, "<lineage>", accum, randFactor)
      |  }
      |  <endif>
      |  res
      |\}""".stripMargin


  override def code(ctx: CodeGenContext, op: Filter): String = {
    val m = Map("out" -> op.outPipeName,
      "in" -> op.inPipeName,
      "lineage" -> op.lineageSignature,
      "pred" -> ScalaEmitter.emitPredicate(CodeGenContext(ctx, Map[String,Any]("schema" -> op.schema)), op.pred))

    render(m)
  }
}

object FilterEmitter {
  lazy val instance = new FilterEmitter
}