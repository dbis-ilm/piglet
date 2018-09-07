package dbis.piglet.op

import dbis.piglet.expr.SpatialJoinPredicate
import dbis.piglet.op.IndexMethod.IndexMethod
import dbis.piglet.op.PartitionMethod.PartitionMethod
import dbis.piglet.schema._

import scala.collection.mutable.ArrayBuffer



case class SpatialJoin(
    private val out: Pipe, 
    private val in: List[Pipe], 
    predicate: SpatialJoinPredicate,
    index: Option[(IndexMethod, List[String])],
    leftParti: Option[(PartitionMethod, List[String])],
    rightParti: Option[(PartitionMethod, List[String])]
  ) extends PigOperator(List(out), in) {
  
  
  override def lineageString: String = {
    s"""SPATIALJOIN%${predicate.toString()}%$index%""" + super.lineageString
  }

  override def constructSchema: Option[Schema] = {
    val newFields = ArrayBuffer[Field]()
    inputs.foreach(p => p.producer.schema match {
      case Some(s) => if(s.isIndexed) {
        newFields ++= s.element.valueType.asInstanceOf[IndexType] // a bag of Indexes
          .valueType.fields // An Index contains tuples with two fields: indexed column and payload
          .last.fType.asInstanceOf[TupleType] // payload is again a tuple
          .fields // fields in each tuple
          .map { f =>
            Field(f.name, f.fType, p.name :: f.lineage)
          }
        } else {
          newFields ++= s.fields map { f =>
            Field(f.name, f.fType, p.name :: f.lineage)
          }
        }
      case None => newFields += Field("", Types.ByteArrayType)
    })
    schema = Some(Schema(BagType(TupleType(newFields.toArray))))
    schema
  }

  override def toString =
    s"""SPATIALJOIN
       |  out = $outPipeName
       |  in = ${inPipeNames.mkString(",")}
       |  inSchema = {${inputs.map(_.producer.schema).mkString(",")}}
       |  outSchema = $schema
       |  predicate = $predicate
       |  index = $index""".stripMargin
//

}