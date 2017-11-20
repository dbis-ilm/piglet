package dbis.piglet.codegen.spark

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.Delay

class DelayEmitter extends CodeEmitter[Delay] {
  override def template: String =
    """val <out> = <in>.mapPartitions({ iter =>
      |  Thread.sleep(<wtime>)
      |  iter.filter{ t =>
      |    val decision = scala.util.Random.nextInt(<sampleFactor>) == 0
      |    <if (profiling)>
      |    if(decision && scala.util.Random.nextInt(randFactor) == 0) {
      |      PerfMonitor.sampleSize(t,"<lineage>", accum)
      |    }
      |    <endif>
      |    decision
      |  }
      |},true)""".stripMargin


  override def code(ctx: CodeGenContext, op: Delay): String = {
    val m = Map("out" -> op.outPipeName,
      "in" -> op.inPipeName,
      "wtime" -> op.wtime.toMillis,
      "sampleFactor" -> op.sampleFactor,
      "lineage" -> op.lineageSignature)

    render(m)
  }
}

object DelayEmitter {
  lazy val instance = new DelayEmitter
}