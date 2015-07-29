package dbis.pig.op

import dbis.pig.schema._

case class RDFLoad(out: Pipe, file: String, grouped: Option[String]) extends PigOperator {
  _outputs = List(out)
  _inputs = List.empty

  schema = if (grouped.isDefined) {
    // Remove the grouping column from the list of possible columns
    val otherColumns = List("subject", "predicate", "object").filterNot(_ == grouped.get)
    val fields = otherColumns.map {
      Field(_, Types.CharArrayType)
    }.toArray
    Some(
      Schema(
        BagType(
          TupleType(
            Array(
              Field(grouped.get, Types.CharArrayType),
              Field("stmts",
                BagType(
                  TupleType(
                    fields))))))))
  } else {
    Some(
      Schema(
        BagType(
          TupleType(
            Array(
              Field("subject", Types.CharArrayType),
              Field("predicate", Types.CharArrayType),
              Field("object", Types.CharArrayType))))))
  }
}
