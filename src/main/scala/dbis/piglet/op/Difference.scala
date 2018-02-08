package dbis.piglet.op

import dbis.piglet.expr.Ref

case class Difference(private val out: Pipe, private val in1: Pipe, private val in2: Pipe,
                      refs1: Option[List[Ref]] = None,
                      refs2: Option[List[Ref]] = None
                     ) extends PigOperator(List(out), List(in1, in2)) {

  override def lineageString: String = {
    s"""DIFFERENCE%""" + super.lineageString
  }

  override def toString =
    s"""DIFFERENCE
       |  out = $outPipeName
       |  ins = ${inPipeNames.mkString(",")},
       |  refs1 = ${refs1.map(_.mkString(",")).getOrElse("--")},
       |  refs2 = ${refs2.map(_.mkString(",")).getOrElse("--")},
       |  inSchema = $inputSchema
       |  outSchema = $schema""".stripMargin

}
