package dbis.piglet.op

import dbis.piglet.expr.Ref

object IndexMethod extends Enumeration {
  type IndexMethod = Value
  val RTREE = Value
  
  def methodName(method: IndexMethod.IndexMethod): String = method match {
    case RTREE => "RTree"
    case _ => throw new IllegalArgumentException(s"unknown index method: $method")
  }
}

import IndexMethod.IndexMethod

case class IndexOp(
  private val out: Pipe,
  private val in: Pipe,
  field: Ref,
  method: IndexMethod,
  params: Seq[String]
) extends PigOperator(out, in) {
  
  override def lineageString = 
    s"""INDEX%${method}%$field%${params.mkString}"""+super.lineageString
  
}