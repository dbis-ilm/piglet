package dbis.pig.op

import dbis.pig.schema._

trait Expr {
  def traverse(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean
  def resultType(schema: Option[Schema]): (String, PigType)
}

abstract class BinaryExpr(left: Expr, right: Expr) extends Expr {
  override def traverse(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) && left.traverse(schema, traverser) && right.traverse(schema, traverser)
  }
}

object Expr {
  /**
   * This function is a traverser function used as parameter to traverse.
   * It checks the (named) fields referenced in the given expression for conformance to
   * the schema.
   *
   * @param schema the schema of the operator
   * @param ex the expression containing fields
   * @return true if all named fields were found, false otherwise
   */
  def checkExpressionConformance(schema: Schema, ex: Expr): Boolean = ex match {
    case RefExpr(r) => r match {
      case NamedField(n) => schema.indexOfField(n) != -1 // TODO: we should produce an error message
      case _ => true
    }
    case _ => true
  }

  /**
   * This function is a traverser function used as parameter to traverse.
   * It checks whether the expression contains any named field.
   *
   * @param schema the schema of the operator
   * @param ex the expression containing fields
   * @return true if all the expression doesn't contain any named field
   */
  def containsNoNamedFields(schema: Schema, ex: Expr): Boolean = ex match {
    case RefExpr(r) => r match {
      case NamedField(n) => false
      case _ => true
    }
    case _ => true
  }


}