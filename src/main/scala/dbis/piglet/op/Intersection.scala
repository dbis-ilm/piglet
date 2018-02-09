package dbis.piglet.op

case class Intersection(out: Pipe, in1: Pipe, in2: Pipe) extends PigOperator(List(out), List(in1, in2)) {

  override def lineageString: String = {
    s"""INTERSECTION%""" + super.lineageString
  }

  override def toString =
    s"""INTERSECTION
       |  out = $outPipeName
       |  ins = ${inPipeNames.mkString(",")}
       |  inSchema = $inputSchema
       |  outSchema = $schema""".stripMargin

}
