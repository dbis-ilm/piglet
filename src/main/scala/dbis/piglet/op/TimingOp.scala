package dbis.piglet.op

import dbis.piglet.schema.Schema
import dbis.piglet.plan.PipeNameGenerator

case class TimingOp (
    private[op] val out: Pipe, 
    private[op] val in: Pipe, 
    operatorId: String) extends PigOperator(out, in, in.producer.schema ) {
  
  override def printOperator(tab: Int): Unit = {
    println(indent(tab)+s"TIMING { out = ${outPipeNames.mkString(",")} , in = ${inPipeNames.mkString(",")}")
    println(indent(tab + 2) + "schema = " + schema)
  }
  
  override def equals(other: Any) = other match {
    case o: TimingOp => operatorId == o.operatorId && outPipeName == o.outPipeName
    case _ => false
  }
  
  override def hashCode() = (operatorId+outPipeName).hashCode()
  
  
  override def toString = s"TIMING { out = ${outPipeNames.mkString(",")} , in = ${inPipeNames.mkString(",")}, schema = $schema }"
  
//  override def lineageString: String = {
//    s"""TIMING%%""" + super.lineageString
//  }
  
}
