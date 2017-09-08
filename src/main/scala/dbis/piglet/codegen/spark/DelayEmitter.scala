package dbis.piglet.codegen.spark

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.Delay

class DelayEmitter extends CodeEmitter[Delay] {
  override def template: String =
    """val <out> = <in>.mapPartitions { iter =>
      |  Thread.sleep(<wtimeMin> + scala.util.Random.nextInt(<wtimeMax> - <wtimeMin>))
      |  iter.filter{ t =>
      |    val decision = scala.util.Random.nextInt(<sampleFactor>) == 0
      |    <if (profiling)>
      |    if(decision && scala.util.Random.nextInt(<randFactor>) == 0) {
      |      accum.incr("<lineage>", PerfMonitor.estimateSize(t))
      |    }
      |    <endif>
      |    decision
      |  }
      |}""".stripMargin


  override def code(ctx: CodeGenContext, op: Delay): String = {
    val m = Map("out" -> op.outPipeName,
      "in" -> op.inPipeName,
      "wtimeMin" -> op.wtime._1.toMillis.toInt,
      "wtimeMax" -> op.wtime._2.toMillis.toInt,
      "sampleFactor" -> op.sampleFactor)

    render(m)
  }
}

object DelayEmitter {
  lazy val instance = new DelayEmitter
}