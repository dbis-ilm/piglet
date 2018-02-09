package dbis.piglet.op

import dbis.piglet.expr.Ref

case class Visualize(private val in: Pipe, field: Ref, path: String, width: Int, height: Int, pointSize: Option[Int] = None) extends PigOperator(List(), List(in)) {


  lazy val (pathNoExt,fileType) = {
    val i = path.lastIndexOf(".")
    if(i > 0) {
      val p = path.substring(0,i)
      val ext = path.substring(i+1)
      (p,ext)
    }
    else
      (path,"")
  }


  override def lineageString: String = {
    s"""VISUALIZE%""" + super.lineageString
  }

  override def toString =
    s"""VISUALIZE
       |  in = $inPipeName
       |  type = $path
       |  size = $width x $height
       |  pointSize = $pointSize""".stripMargin

}
