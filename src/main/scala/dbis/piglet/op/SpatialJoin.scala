package dbis.piglet.op

import dbis.piglet.expr.Ref
import dbis.piglet.schema.Schema
import dbis.piglet.schema.Field
import scala.collection.mutable.ArrayBuffer
import dbis.piglet.schema.BagType
import dbis.piglet.schema.TupleType
import dbis.piglet.schema.Types
import dbis.piglet.expr.SpatialJoinPredicate



case class SpatialJoin(
    private val out: Pipe, 
    private val in: List[Pipe], 
    predicate: SpatialJoinPredicate,
    withIndex: Boolean
  ) extends PigOperator(List(out), in) {
  
  
  override def lineageString: String = {
    s"""JOIN%${predicate.toString()}%index=${withIndex}%""" + super.lineageString
  }

  override def constructSchema: Option[Schema] = {
    val newFields = ArrayBuffer[Field]()
    inputs.foreach(p => p.producer.schema match {
      case Some(s) => newFields ++= s.fields map { f =>
        Field(f.name, f.fType, p.name :: f.lineage)
      }
      case None => newFields += Field("", Types.ByteArrayType)
    })
    schema = Some(Schema(BagType(TupleType(newFields.toArray))))
    schema
  }

  override def printOperator(tab: Int): Unit = {
    println(indent(tab) + s"SPATIALJOIN { out = ${outPipeNames.mkString(",")} , in = ${inPipeNames.mkString(",")} }")
    println(indent(tab + 2) + s"predicate: ${predicate.toString()}")
    println(indent(tab + 2) + "inSchema = {}")
    println(indent(tab + 2) + "outSchema = " + schema)
  }
  
  
}