package dbis.pig.op

import dbis.pig.expr.Ref

object IndexMethod extends Enumeration {
  type IndexMethod = Value
  val RTREE = Value
  
  def methodName(method: IndexMethod.IndexMethod): String = method match {
    case RTREE => "RTree"
    case _ => throw new IllegalArgumentException(s"unknown index method: $method")
  }
}

import IndexMethod.IndexMethod
import dbis.pig.schema.Schema
import dbis.pig.schema.BagType
import dbis.pig.schema.TupleType
import dbis.pig.schema.Field
import dbis.pig.schema.PigType
import dbis.pig.schema.SimpleType
import dbis.pig.schema.Types

case class IndexOp(
  private val out: Pipe,
  private val in: Pipe,
  field: Ref,
  method: IndexMethod,
  params: Seq[String]
) extends PigOperator(
    List(out), List(in) 
//    Some(
//      Schema(
//        BagType(
//          TupleType(
//            Array(
//              Field("idx", 
//                TupleType(
//                  Array(
//                    Field("geom", Types.ByteArrayType), // in.producer.schema.map { s => s.f, Types.ByteArrayType)
//                    Field("orig", 
//                      TupleType(
//                        in.producer.schema.map { s => s.fields }.getOrElse(Array())
//                      )
//                    )
//                  )
//                )
//              )              
//            ), 
//            IndexMethod.methodName(method)
//          )
//        )
//      )
//    )
////       RDD[RTree[Geometry, (Geometry, importer._t11_Tuple)]]
  ) {
  
  override def lineageString = 
    s"""INDEX%${method}%$field%${params.mkString}"""+super.lineageString
  
}