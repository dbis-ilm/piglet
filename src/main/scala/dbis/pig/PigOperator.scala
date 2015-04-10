package dbis.pig

/**
 * Created by kai on 31.03.15.
 */

sealed abstract class PigOperator (val outPipeName: String, val inPipeNames: List[String]) {
  var inputs: List[Pipe] = List[Pipe]()
  var output: Option[Pipe] = None
  var schema: Option[Schema] = None

  def this(out: String) = this(out, List())

  def this(out: String, in: String) = this(out, List(in))

  def constructSchema: Option[Schema] = {
    if (inputs.nonEmpty)
      schema = inputs(0).producer.schema
    schema
  }
}

case class Load(override val outPipeName: String, file: String) extends PigOperator(outPipeName) {
  override def constructSchema: Option[Schema] = {
    // schema = inputs(0).producer.schema // TODO
    None
  }
}

case class Dump(inPipeName: String) extends PigOperator("", inPipeName)

case class Foreach(override val outPipeName: String, inPipeName: String, expr: List[String])
  extends PigOperator(outPipeName, inPipeName) {
  override def constructSchema: Option[Schema] = {
    // schema = inputs(0).producer.schema // TODO
    schema
  }
}


case class Filter(override val outPipeName: String, inPipeName: String, pred: Predicate)
  extends PigOperator(outPipeName, inPipeName)


sealed abstract class Ref

case class NamedField(name: String) extends Ref
case class PositionalField(pos: Int) extends Ref
case class Value(v: Any) extends Ref

sealed abstract class Predicate

case class Eq(a: Ref, b: Ref) extends Predicate
case class Neq(a: Ref, b: Ref) extends Predicate
case class Geq(a: Ref, b: Ref) extends Predicate
case class Leq(a: Ref, b: Ref) extends Predicate
case class Gt(a: Ref, b: Ref) extends Predicate
case class Lt(a: Ref, b: Ref) extends Predicate
case class And(a: Ref, b: Ref) extends Predicate
case class Or(a: Ref, b: Ref) extends Predicate
case class Not(a: Ref) extends Predicate