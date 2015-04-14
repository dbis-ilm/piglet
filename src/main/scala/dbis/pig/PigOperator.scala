package dbis.pig

/**
 * Created by kai on 31.03.15.
 */

import java.security.MessageDigest

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

  /**
   * Returns a MD5 hash string representing the sub-plan producing the input for this operator.
   *
   * @return the MD5 hash string
   */
  def lineageSignature: String = {
    val digest = MessageDigest.getInstance("MD5")
    digest.digest(lineageString.getBytes).map("%02x".format(_)).mkString
  }

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  def lineageString: String = {
    inputs.map(p => p.producer.lineageString).mkString("%")
  }
}

/**
 * Load represents the LOAD operator of Pig.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param file the name of the file to be loaded
 */
case class Load(override val outPipeName: String, file: String,
                loaderFunc: String = "", loaderParams: List[String] = null) extends PigOperator(outPipeName) {
  override def constructSchema: Option[Schema] = {
    // schema = inputs(0).producer.schema // TODO
    None
  }

  override def lineageString: String = {
    s"""LOAD%${file}%""" + super.lineageString
  }
}

/**
 * Dump represents the DUMP operator of Pig.
 *
 * @param inPipeName the name of the input pipe
 */
case class Dump(inPipeName: String) extends PigOperator("", inPipeName) {
  override def lineageString: String = {
    s"""DUMP%""" + super.lineageString
  }
}

/**
 * Store represents the STORE operator of Pig.
 *
 * @param inPipeName the name of the input pipe
 * @param file the name of the output file
 */
case class Store(inPipeName: String, file: String) extends PigOperator("", inPipeName) {
  override def lineageString: String = {
    s"""STORE%${file}%""" + super.lineageString
  }
}

/**
 * Describe represents the DESCRIBE operator of Pig.
 *
 * @param inPipeName the name of the input pipe
 */
case class Describe(inPipeName: String) extends PigOperator("", inPipeName) {
  override def lineageString: String = {
    s"""DESCRIBE%""" + super.lineageString
  }

}

case class GeneratorExpr(expr: ArithmeticExpr, alias: Option[String] = None)

/**
 * Foreach represents the FOREACH operator of Pig.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param inPipeName the name of the input pipe
 * @param expr the generator expression
 */
case class Foreach(override val outPipeName: String, inPipeName: String, expr: List[GeneratorExpr])
  extends PigOperator(outPipeName, inPipeName) {
  override def constructSchema: Option[Schema] = {
    // schema = inputs(0).producer.schema // TODO
    schema
  }

  override def lineageString: String = {
    s"""FOREACH%${expr}%""" + super.lineageString
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
  extends PigOperator(outPipeName, inPipeName) {
  override def lineageString: String = {
    s"""FILTER%${pred}%""" + super.lineageString
  }

}

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
  extends PigOperator(outPipeName, inPipeName) {
  override def lineageString: String = {
    s"""GROUPBY%${groupExpr}%""" + super.lineageString
  }

}

/**
 * Distinct represents the DISTINCT operator of Pig.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param inPipeName the name of the input pipe.
 */
case class Distinct(override val outPipeName: String, inPipeName: String)
  extends PigOperator(outPipeName, inPipeName) {
  override def lineageString: String = {
    s"""DISTINCT%""" + super.lineageString
  }

}

/**
 * Limit represents the LIMIT operator of Pig.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param inPipeName the name of the input pipe.
 */
case class Limit(override val outPipeName: String, inPipeName: String, num: Int)
  extends PigOperator(outPipeName, inPipeName) {
  override def lineageString: String = {
    s"""LIMIT%${num}%""" + super.lineageString
  }

}

/**
 * Join represents the multiway JOIN operator of Pig.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param inPipeNames the list of names of input pipes.
 * @param fieldExprs  list of key expressions (list of keys) used as join expressions.
 */
case class Join(override val outPipeName: String, override val inPipeNames: List[String], val fieldExprs: List[List[Ref]])
  extends PigOperator(outPipeName, inPipeNames) {
  override def lineageString: String = {
    s"""JOIN%""" + super.lineageString
  }

}