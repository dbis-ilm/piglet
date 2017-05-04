package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.TimingOp

class TimingEmitter extends CodeEmitter[TimingOp] {
  override def template = """val <out> = <in>.mapPartitionsWithIndex({case (idx,iter) => 
    |  PerfMonitor.notify(url, "<lineage>", <in>,idx, System.currentTimeMillis)
    |  iter
    \},true)""".stripMargin
  
  override def code(ctx: CodeGenContext, op: TimingOp): String = render(Map(
      "out"-> op.outPipeName, 
      "in" -> op.inPipeName,
      "lineage" -> op.operatorId))
}

object TimingEmitter {
	lazy val instance = new TimingEmitter
}