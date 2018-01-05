package dbis.piglet.op

import dbis.piglet.expr.{NamedField, Ref, RefExpr}
import dbis.piglet.schema._

object IndexMethod extends Enumeration {
  type IndexMethod = Value
  val RTREE = Value
  
  def methodName(method: IndexMethod.IndexMethod): String = method match {
    case RTREE => "RTree"
    case _ => throw new IllegalArgumentException(s"unknown index method: $method")
  }
}

import dbis.piglet.op.IndexMethod.IndexMethod

case class IndexOp(
  private val out: Pipe,
  private val in: Pipe,
  field: Ref,
  method: IndexMethod,
  params: Seq[String]
) extends PigOperator(out, in) {


  override def constructSchema = {
    val inSchema = inputs.head.producer.schema

    val inputType = inSchema match {
      case Some(s) => s.element.valueType
      case None => TupleType(Array(Field("", Types.ByteArrayType)))
    }

    val keyField = field match {
      case nf:NamedField =>
        Field(nf.name, RefExpr(field).resultType(inSchema), nf.lineage)
      case _ =>
        Field("", RefExpr(field).resultType(inSchema))
    }

    val nested = Field(in.name, inputType)
    val fields = Array(keyField, nested)

    val iBag = BagType(IndexType(TupleType(fields), IndexMethod.methodName(method)))

    schema = Some(Schema(iBag))
    schema
  }


  override def lineageString = 
    s"""INDEX%$method%$field%${params.mkString}"""+super.lineageString

  override def toString =
    s"""INDEX
       |  out = $outPipeName
       |  in = $inPipeName
       |  field = $field
       |  index method = $method
       |  params = ${params.mkString(",")}
     """.stripMargin
  
}