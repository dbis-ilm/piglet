package dbis.piglet.op

import dbis.piglet.schema.Schema
import dbis.piglet.plan.PipeNameGenerator

case class TimingOp (
    private[op] val out: Pipe, 
    private[op] val in: Pipe, 
    operatorId: String) extends PigOperator(out, in, schema = in.inputSchema) {
  
  
  override def printOperator(tab: Int): Unit = {
    println(indent(tab) + s"TIMING { out = ${outPipeName} , in = ${inPipeNames.mkString(",")} }")
    println(indent(tab + 2) + "schema = " + inputSchema)
  }
  
  override def equals(other: Any) = other match {
    case o: TimingOp => operatorId == o.operatorId  
    case _ => false
  }
  
  override def hashCode() = operatorId.hashCode()
  
  
  
  
//  override def lineageString: String = {
//    s"""TIMING%%""" + super.lineageString
//  }
  
}
