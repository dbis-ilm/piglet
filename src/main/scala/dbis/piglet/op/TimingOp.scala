package dbis.piglet.op

import dbis.piglet.schema.Schema
import dbis.piglet.plan.PipeNameGenerator

case class TimingOp (
    private[op] val out: Pipe, 
    private[op] val in: Pipe, 
    operatorId: String) extends PigOperator(out, in) {
  
  
  override def printOperator(tab: Int): Unit = {
    println(indent(tab) + s"TIMING { out = ${outPipeName} , in = ${inPipeNames.mkString(",")} }")
  }
  
}