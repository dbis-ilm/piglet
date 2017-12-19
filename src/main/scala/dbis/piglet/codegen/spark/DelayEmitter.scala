package dbis.piglet.codegen.spark

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.Delay

class DelayEmitter extends CodeEmitter[Delay] {
  override def template: String =
    """val <out> = <in>.mapPartitions({ iter =>
      |  Thread.sleep(<wtime>)
      |  <processor>
      |},true)""".stripMargin


  lazy val processorFilterTemplate = s"""iter.filter{ t =>
    |
    |  val decision = scala.util.Random.nextInt(<sampleFactor>) == 0
    |  <if (profiling)>
    |  if(decision)
    |    PerfMonitor.sampleSize(t,"<lineage>", accum, randFactor)
    |  <endif>
    |  decision
    |}""".stripMargin

  lazy val processorDuplTemplate = s"""iter.flatMap{ t =>
    |  (0 until <sampleFactor>).iterator.map{_ =>
    |    <if (profiling)>
    |    PerfMonitor.sampleSize(t,"<lineage>", accum, randFactor)
    |    <endif>
    |    t
    |  }
    |}""".stripMargin


  override def code(ctx: CodeGenContext, op: Delay): String = {

    val processorParams = Map(
      "sampleFactor" -> math.abs(op.sampleFactor), // always use positive value
      "lineage" -> op.lineageSignature
    )

    // if sampleFactor is negative, use a filter to reduce tuples, otherwise duplicate them
    val processorCode = if(op.sampleFactor < 0 )
      CodeEmitter.render(processorFilterTemplate, processorParams)
    else CodeEmitter.render(processorDuplTemplate, processorParams)

    val m = Map("out" -> op.outPipeName,
      "in" -> op.inPipeName,
      "wtime" -> op.wtime.toMillis,
      "processor" -> processorCode
      )

    render(m)
  }
}

object DelayEmitter {
  lazy val instance = new DelayEmitter
}