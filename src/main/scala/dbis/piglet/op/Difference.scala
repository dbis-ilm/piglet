package dbis.piglet.op

case class Difference(out: Pipe, in1: Pipe, in2: Pipe) extends PigOperator(List(out), List(in1, in2)) {

  override def lineageString: String = {
    s"""DIFFERENCE%""" + super.lineageString
  }

  override def toString =
    s"""DIFFERENCE
       |  out = $outPipeName
       |  ins = ${inPipeNames.mkString(",")}
       |  inSchema = $inputSchema
       |  outSchema = $schema""".stripMargin

}
