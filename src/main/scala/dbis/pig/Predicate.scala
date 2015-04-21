package dbis.pig

trait Expr {
  def traverse(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean
  def resultType(schema: Option[Schema]): (String, PigType)
}

abstract class BinaryExpr(left: Expr, right: Expr) extends Expr {
  override def traverse(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) && left.traverse(schema, traverser) && right.traverse(schema, traverser)
  }
}

trait ArithmeticExpr extends Expr


sealed abstract class Ref

case class NamedField(name: String) extends Ref

case class PositionalField(pos: Int) extends Ref

case class Value(v: Any) extends Ref

case class DerefTuple(tref: Ref, component: Ref) extends Ref

case class DerefMap(mref: Ref, key: String) extends Ref

case class RefExpr(r: Ref) extends ArithmeticExpr {
  override def traverse(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this)
  }

  override def resultType(schema: Option[Schema]): (String, PigType) = schema match {
    case Some(s) => r match {
        case NamedField(n) => val f = s.field(n); (n, f.fType)
        case PositionalField(p) => val f = s.field(p); ("", Types.ByteArrayType)
        case Value(v) => if (v.isInstanceOf[String]) ("", Types.CharArrayType) else ("", Types.ByteArrayType)
        // TODO: handle deref of tuple, bag
      //case DerefTuple(t, c) =>
      //case DerefMap(m, k) =>
      }
      case None => ("", Types.ByteArrayType)
    }
}

case class CastExpr(t: PigType, a: ArithmeticExpr) extends ArithmeticExpr {
  override def traverse(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) && a.traverse(schema, traverser)
  }

  override def resultType(schema: Option[Schema]): (String, PigType) = (a.resultType(schema)._1, t)
}

case class MSign(a: ArithmeticExpr) extends ArithmeticExpr {
  override def traverse(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) && a.traverse(schema, traverser)
  }
  override def resultType(schema: Option[Schema]): (String, PigType) = a.resultType(schema)
}

case class Add(left: ArithmeticExpr, right: ArithmeticExpr) extends BinaryExpr(left, right) with ArithmeticExpr {
  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.DoubleType)
}

case class Minus(left: ArithmeticExpr, right: ArithmeticExpr) extends BinaryExpr(left, right) with ArithmeticExpr {
  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.DoubleType)
}

case class Mult(left: ArithmeticExpr, right: ArithmeticExpr) extends BinaryExpr(left, right) with ArithmeticExpr {
  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.DoubleType)
}

case class Div(left: ArithmeticExpr, right: ArithmeticExpr) extends BinaryExpr(left, right) with ArithmeticExpr {
  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.DoubleType)
}

case class Func(f: String, params: List[ArithmeticExpr]) extends ArithmeticExpr {
  override def traverse(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) &&
      params.map(_.traverse(schema, traverser)).foldLeft(true){ (b1: Boolean, b2: Boolean) => b1 && b2 }
  }

  // TODO: we should know the function signature
    override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.ByteArrayType)
}

trait Predicate extends Expr

case class Eq(left: ArithmeticExpr, right: ArithmeticExpr) extends BinaryExpr(left, right) with Predicate {
  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.BooleanType)
}

case class Neq(left: ArithmeticExpr, right: ArithmeticExpr) extends BinaryExpr(left, right) with Predicate {
  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.BooleanType)
}

case class Geq(left: ArithmeticExpr, right: ArithmeticExpr) extends BinaryExpr(left, right) with Predicate {
  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.BooleanType)
}

case class Leq(left: ArithmeticExpr, right: ArithmeticExpr) extends BinaryExpr(left, right) with Predicate {
  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.BooleanType)
}

case class Gt(left: ArithmeticExpr, right: ArithmeticExpr) extends BinaryExpr(left, right) with Predicate {
  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.BooleanType)
}

case class Lt(left: ArithmeticExpr, right: ArithmeticExpr) extends BinaryExpr(left, right) with Predicate {
  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.BooleanType)
}

/* TODO: should work with Predicate as operands */
case class And(a: Ref, b: Ref) extends Predicate {
  override def traverse(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = true

  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.BooleanType)
}

case class Or(a: Ref, b: Ref) extends Predicate {
  override def traverse(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = true

  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.BooleanType)
}

case class Not(a: Ref) extends Predicate {
  override def traverse(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = true

  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.BooleanType)
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