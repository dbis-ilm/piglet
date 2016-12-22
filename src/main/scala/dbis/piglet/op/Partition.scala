package dbis.piglet.op

object PartitionMethod extends Enumeration {
  type PartitionMethod = Value
  val GRID, BSP = Value
}

import PartitionMethod.PartitionMethod
import dbis.piglet.expr.Ref

case class Partition(
  private val out: Pipe,
  private val in: Pipe,
  field: Ref,
  method: PartitionMethod,
  params: Seq[String]
) extends PigOperator(out, in) {
  
  override def lineageString = 
    s"""PARTITION%${method}%$field%${params.mkString}"""+super.lineageString
  
}