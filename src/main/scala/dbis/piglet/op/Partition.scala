package dbis.piglet.op

object PartitionMethod extends Enumeration {
  type PartitionMethod = Value
  val GRID, BSP, Hash = Value
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
    s"""PARTITION%$method%$field%${params.mkString}"""+super.lineageString

  override def toString =
    s"""PARTITION
       |  out = $outPipeName
       |  in = $inPipeName
       |  schema = $schema
       |  field = $field
       |  method = $method
       |  params = ${params.mkString(",")}""".stripMargin
}