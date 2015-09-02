package dbis.pig.op

import dbis.pig.schema._
import java.net.URI
import scala.collection.mutable.Map

case class RDFLoad(out: Pipe, uri: URI, grouped: Option[String]) extends PigOperator {
  _outputs = List(out)
  _inputs = List.empty

  schema = if (grouped.isDefined) {
    if (RDFLoad.groupedSchemas.contains(grouped.get)){
      Some(RDFLoad.groupedSchemas(grouped.get))
    }
    else {
      throw new IllegalArgumentException(grouped.get + " is not a valid RDF grouping column")
    }
  } else {
    RDFLoad.plainSchema
  }

  def BGPFilterIsReachable: Boolean = {
    def isBGPFilter(op: PigOperator): Boolean = op match {
      case _: BGPFilter => true
      case _ => op.outputs.flatMap(_.consumer).map(isBGPFilter).exists(_ == true)
    }
    isBGPFilter(this)
  }
}

object RDFLoad {
  /** The schema for plain RDF data
    *
    */
  final val plainSchema: Some[Schema] = Some(
    Schema(
      BagType(
        TupleType(
          Array(
            Field("subject", Types.CharArrayType),
            Field("predicate", Types.CharArrayType),
            Field("object", Types.CharArrayType))))))

  /** A map of column names to the schema where the data is grouped by that column.
    *
    */
  lazy final val groupedSchemas = {
    var m = Map[String, Schema]()
    val columns = List[String]("subject", "predicate", "object")
    for (grouping_column <- columns) {
      val fields = columns.filterNot(_ == grouping_column).map {
        Field(_, Types.CharArrayType)
      }.toArray
      m(grouping_column) = Schema(
        BagType(
          TupleType(
            Array(
              Field(grouping_column, Types.CharArrayType),
              Field("stmts",
                BagType(
                  TupleType(
                    fields)))))))
    }
    m
  }
}
