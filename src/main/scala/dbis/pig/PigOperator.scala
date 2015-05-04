package dbis.pig

/**
 * Created by kai on 31.03.15.
 */

import java.security.MessageDigest

import scala.collection.mutable.ArrayBuffer

/**
 * PigOperator is the base class for all Pig operators. An operator contains
 * pipes representing the input and output connections to other operators in the
 * dataflow.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param inPipeNames the list of names of input pipes.
 */
sealed abstract class PigOperator (val outPipeName: String, val inPipeNames: List[String], var schema: Option[Schema]) {
  var inputs: List[Pipe] = List[Pipe]()
  var output: Option[Pipe] = None

  def this(out: String, in: List[String]) = this(out, in, None)

  def this(out: String) = this(out, List(), None)

  def this(out: String, in: String) = this(out, List(in), None)

  /**
   * Constructs the output schema of this operator based on the input + the semantics of the operator.
   * The default implementation is to simply take over the schema of the input operator.
   *
   * @return the output schema
   */
  def constructSchema: Option[Schema] = {
    if (inputs.nonEmpty)
      schema = inputs.head.producer.schema
    schema
  }

  /**
   * Returns a string representation of the output schema of the operator.
   *
   * @return a string describing the schema
   */
  def schemaToString: String = {
    /*
     * schemaToString is mainly called from DESCRIBE. Thus, we can take outPipeName as relation name.
     */
    schema match {
      case Some(s) => s"${outPipeName}: ${s.element.descriptionString}"
      case None => s"Schema for ${outPipeName} unknown."
    }
  }

  /**
   * A helper function for traversing expression trees:
   *
   * Checks the (named) fields referenced in the expression (if any) if they conform to
   * the schema. Should be overridden in operators changing the schema by invoking
   * traverse with one of the traverser function.
   *
   * @return true if valid field references, otherwise false
   */
  def checkSchemaConformance: Boolean = true

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
                var loadSchema: Option[Schema] = None,
                loaderFunc: String = "", loaderParams: List[String] = null) extends PigOperator(outPipeName, List(), loadSchema) {
  override def constructSchema: Option[Schema] = {
    /*
     * Either the schema was defined or it is None.
     */
    schema
  }

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
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

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
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

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""STORE%${file}%""" + super.lineageString
  }
}

/**
 * Register represents a pseudo operator for the REGISTER statement. This "operator" will
 * be eliminated during building the dataflow plan.
 *
 * @param jarFile the URI of the Jar file to be registered
 */
case class Register(jarFile: String) extends PigOperator("")

/**
 * Describe represents the DESCRIBE operator of Pig.
 *
 * @param inPipeName the name of the input pipe
 */
case class Describe(inPipeName: String) extends PigOperator("", inPipeName) {

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""DESCRIBE%""" + super.lineageString
  }

}

case class GeneratorExpr(expr: ArithmeticExpr, alias: Option[Field] = None)

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
    val inputSchema = inputs.head.producer.schema
    // we create a bag of tuples containing fields for each expression in expr
    val fields = expr.map(e => {
      e.alias match {
        // if we have an explicit schema (i.e. a field) then we use it
        case Some(f) => {
          if (f.fType == Types.ByteArrayType) {
            // if the type was only bytearray, we should check the expression if we have a more
            // specific type
            val res = e.expr.resultType(inputSchema)
            Field(f.name, res._2)
          }
          else
            f
        }
        // otherwise we take the field name from the expression and
        // the input schema
        case None => val res = e.expr.resultType(inputSchema); Field(res._1, res._2)
      }
    }).toArray
    schema = Some(new Schema(new BagType("", new TupleType("", fields))))
    schema
  }

  override def checkSchemaConformance: Boolean = {
    schema match {
      case Some(s) => {
        // if we know the schema we check all named fields
        expr.map(_.expr.traverse(s, Expr.checkExpressionConformance)).foldLeft(true)((b1: Boolean, b2: Boolean) => b1 && b2)
      }
      case None => {
        // if we don't have a schema all expressions should contain only positional fields
        expr.map(_.expr.traverse(null, Expr.containsNoNamedFields)).foldLeft(true)((b1: Boolean, b2: Boolean) => b1 && b2)
      }
    }
  }

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
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

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""FILTER%${pred}%""" + super.lineageString
  }

  override def checkSchemaConformance: Boolean = {
    schema match {
      case Some(s) => {
        // if we know the schema we check all named fields
        pred.traverse(s, Expr.checkExpressionConformance)
      }
      case None => {
        // if we don't have a schema all expressions should contain only positional fields
        pred.traverse(null, Expr.containsNoNamedFields)
      }
    }
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

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""GROUPBY%${groupExpr}%""" + super.lineageString
  }

  override def constructSchema: Option[Schema] = {
    val inputSchema = inputs.head.producer.schema
    // tuple(group: typeOfGroupingExpr, in:bag(inputSchema))
    val inputType = inputSchema match {
      case Some(s) => s.element.valueType
      case None => TupleType("", Array(Field("", Types.ByteArrayType)))
    }
    val groupingType = Types.IntType
    val fields = Array(Field("group", groupingType),
                      Field(inputs.head.name, BagType("", inputType)))
    schema = Some(new Schema(new BagType("", new TupleType("", fields))))
    schema
  }

  override def checkSchemaConformance: Boolean = {
    schema match {
      case Some(s) => {
        // if we know the schema we check all named fields
        groupExpr.keyList.filter(_.isInstanceOf[NamedField]).exists(f => s.indexOfField(f.asInstanceOf[NamedField].name) != -1)
      }
      case None => {
        // if we don't have a schema all expressions should contain only positional fields
        groupExpr.keyList.map(_.isInstanceOf[NamedField]).exists(b => b)
      }
    }
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
 * Union represents the UNION operator of Pig.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param inPipeNames the list of names of input pipes.
 */
case class Union(override val outPipeName: String, override val inPipeNames: List[String])
  extends PigOperator(outPipeName, inPipeNames) {
  override def lineageString: String = {
    s"""UNION%""" + super.lineageString
  }

  override def constructSchema: Option[Schema] = {
    val bagType = (p: Pipe) => p.producer.schema.get.element
    val generalizedBagType = (b1: BagType, b2: BagType) => {
      require(b1.valueType.fields.length == b2.valueType.fields.length)
      val newFields = ArrayBuffer[Field]()
      val fieldPairs = b1.valueType.fields.zip(b2.valueType.fields)
      for ((f1, f2) <- fieldPairs) {
        newFields += Field(f1.name, Types.escalateTypes(f1.fType, f2.fType))
      }
      BagType(b1.s, TupleType(b1.valueType.s, newFields.toArray))
    }

    // case 1: one of the input schema isn't known -> output schema = None
    if (inputs.exists(p => p.producer.schema == None)) {
      schema = None
    }
    else {
      // case 2: all input schemas have the same number of fields
      val s1 = inputs.head.producer.schema.get
      if (! inputs.tail.exists(p => s1.fields.length != p.producer.schema.get.fields.length)) {
        val typeList = inputs.map(p => bagType(p))
        val resultType = typeList.reduceLeft(generalizedBagType)
        schema = Some(new Schema(resultType))
      }
      else {
        // case 3: the number of fields differ
        schema = None
      }
    }
    schema
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

  override def constructSchema: Option[Schema] = {
    val newFields = ArrayBuffer[Field]()
    inputs.foreach(p => p.producer.schema match {
      case Some(s) => newFields ++= s.fields
      case None => ???
    })
    schema = Some(new Schema(BagType("", TupleType("", newFields.toArray))))
    schema
  }

}

case class StreamOp(override val outPipeName: String, inPipeName: String, opName: String, var loadSchema: Option[Schema] = None)
  extends PigOperator(outPipeName, List(inPipeName), loadSchema) {
  override def lineageString: String = s"""STREAM%""" + super.lineageString

  override def checkSchemaConformance: Boolean = {
    // TODO
    true
  }

  override def constructSchema: Option[Schema] = {
    // TODO
    schema
  }
}

case class Sample(override val outPipeName: String, inPipeName: String, expr: ArithmeticExpr)
  extends PigOperator(outPipeName, inPipeName) {
  override def lineageString: String = s"""SAMPLE%""" + super.lineageString

  override def checkSchemaConformance: Boolean = {
    schema match {
      case Some(s) => {
        // if we know the schema we check all named fields
        expr.traverse(s, Expr.checkExpressionConformance)
      }
      case None => {
        // if we don't have a schema all expressions should contain only positional fields
        expr.traverse(null, Expr.containsNoNamedFields)
      }
    }
  }
}

object OrderByDirection extends Enumeration {
  type OrderByDirection = Value
  val AscendingOrder, DescendingOrder = Value
}

import OrderByDirection._

case class OrderBySpec(field: Ref, dir: OrderByDirection)

case class OrderBy(override val outPipeName: String, inPipeName: String, orderSpec: List[OrderBySpec])
  extends PigOperator(outPipeName, inPipeName) {
  override def lineageString: String = s"""ORDERBY%""" + super.lineageString

  override def checkSchemaConformance: Boolean = {
    // TODO
    true
  }

  override def constructSchema: Option[Schema] = {
    // TODO
    schema
  }
}