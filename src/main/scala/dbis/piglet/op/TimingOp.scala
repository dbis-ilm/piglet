package dbis.piglet.op

case class TimingOp (
    private[op] val out: Pipe, 
    private[op] val in: Pipe, 
    operatorId: String) extends PigOperator(out, in, in.producer.schema ) {
  

  
  override def equals(other: Any) = other match {
    case o: TimingOp => operatorId == o.operatorId && outPipeName == o.outPipeName
    case _ => false
  }
  
  override def hashCode() = (operatorId+outPipeName).hashCode()

  override def toString =
    s"""TIMING
       |  out = $outPipeName
       |  in = $inPipeName
       |  schema = $schema""".stripMargin

  
}
