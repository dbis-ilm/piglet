package dbis.pig.op

import dbis.pig.schema._

case class RDFLoad(out: Pipe, file: String, grouped: Option[String]) extends PigOperator {
  _outputs = List(out)
  _inputs = List.empty
  // TODO set the schema depending on `grouped`
  schema = null
}
