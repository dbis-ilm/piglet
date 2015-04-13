package dbis.pig

/**
 * Created by kai on 31.03.15.
 */

/**
 * PigOperator is the base class for all Pig operators. An operator contains
 * pipes representing the input and output connections to other operators in the
 * dataflow.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param inPipeNames the list of names of input pipes.
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

  def schemaToString: String = {
    "" // TODO: how to describe the schema??
  }
}

/**
 * Load represents the LOAD operator of Pig.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param file the name of the file to be loaded
 */
case class Load(override val outPipeName: String, file: String) extends PigOperator(outPipeName) {
  override def constructSchema: Option[Schema] = {
    // schema = inputs(0).producer.schema // TODO
    None
  }
}

/**
 * Dump represents the DUMP operator of Pig.
 *
 * @param inPipeName the name of the input pipe
 */
case class Dump(inPipeName: String) extends PigOperator("", inPipeName)

/**
 * Describe represents the DESCRIBE operator of Pig.
 *
 * @param inPipeName the name of the input pipe
 */
case class Describe(inPipeName: String) extends PigOperator("", inPipeName)

/**
 * Foreach represents the FOREACH operator of Pig.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param inPipeName the name of the input pipe
 * @param expr the generator expression
 */
case class Foreach(override val outPipeName: String, inPipeName: String, expr: List[String])
  extends PigOperator(outPipeName, inPipeName) {
  override def constructSchema: Option[Schema] = {
    // schema = inputs(0).producer.schema // TODO
    schema
  }
}

/**
 * Filter represents the FILTER operator of Pig.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param inPipeName the name of the input pipe
 * @param pred the predicate used for filtering tuples from the input pipe
 */
case class Filter(override val outPipeName: String, inPipeName: String, pred: Predicate)
  extends PigOperator(outPipeName, inPipeName)

/**
 * Represents the grouping expression for the Grouping operator.
 *
 * @param keyList a list of keys used for grouping
 */
case class GroupingExpression(val keyList: List[Ref])

/**
 * Grouping represents the GROUP ALL / GROUP BY operator of Pig.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param inPipeName the name of the input pipe
 * @param groupExpr the expression (a key or a list of keys) used for grouping
 */
case class Grouping(override val outPipeName: String, inPipeName: String, groupExpr: GroupingExpression)
  extends PigOperator(outPipeName, inPipeName)

/**
 * Distinct represents the DISTINCT operator of Pig.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param inPipeName the name of the input pipe.
 */
case class Distinct(override val outPipeName: String, inPipeName: String)
  extends PigOperator(outPipeName, inPipeName)

/**
 * Join represents the multiway JOIN operator of Pig.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param inPipeNames the list of names of input pipes.
 * @param fieldExprs  list of key expressions (list of keys) used as join expressions.
 */
case class Join(override val outPipeName: String, override val inPipeNames: List[String], val fieldExprs: List[List[Ref]])
  extends PigOperator(outPipeName, inPipeNames)