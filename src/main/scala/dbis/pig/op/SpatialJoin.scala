package dbis.pig.op

import dbis.pig.expr.Ref
import dbis.pig.schema.Schema
import dbis.pig.schema.Field
import scala.collection.mutable.ArrayBuffer
import dbis.pig.schema.BagType
import dbis.pig.schema.TupleType
import dbis.pig.schema.Types
import dbis.pig.expr.SpatialPredicate



case class SpatialJoin(
    out: Pipe, 
    in: List[Pipe], 
    predicate: SpatialPredicate
  ) extends PigOperator {
  
  _outputs = List(out)
  _inputs = in
  
  override def lineageString: String = {
    s"""JOIN%${predicate.toString()}%""" + super.lineageString
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
    println(indent(tab) + s"JOIN { out = ${outPipeNames.mkString(",")} , in = ${inPipeNames.mkString(",")} }")
    println(indent(tab + 2) + "inSchema = {}")
    println(indent(tab + 2) + "outSchema = " + schema)
  }
  
  
}