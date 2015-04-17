package dbis.pig

sealed abstract class Ref

case class NamedField(name: String) extends Ref
case class PositionalField(pos: Int) extends Ref
case class Value(v: Any) extends Ref

trait Expr {

}

sealed abstract class ArithmeticExpr extends Expr

case class RefExpr(r: Ref) extends ArithmeticExpr
case class CastExpr(t: String, a: ArithmeticExpr) extends ArithmeticExpr
case class MSign(a: ArithmeticExpr) extends ArithmeticExpr
case class Add(a: ArithmeticExpr, b: ArithmeticExpr) extends ArithmeticExpr
case class Minus(a: ArithmeticExpr, b: ArithmeticExpr) extends ArithmeticExpr
case class Mult(a: ArithmeticExpr, b: ArithmeticExpr) extends ArithmeticExpr
case class Div(a: ArithmeticExpr, b: ArithmeticExpr) extends ArithmeticExpr
case class Func(f: String, params: List[ArithmeticExpr]) extends ArithmeticExpr

sealed abstract class Predicate extends Expr

case class Eq(a: ArithmeticExpr, b: ArithmeticExpr) extends Predicate
case class Neq(a: ArithmeticExpr, b: ArithmeticExpr) extends Predicate
case class Geq(a: ArithmeticExpr, b: ArithmeticExpr) extends Predicate
case class Leq(a: ArithmeticExpr, b: ArithmeticExpr) extends Predicate
case class Gt(a: ArithmeticExpr, b: ArithmeticExpr) extends Predicate
case class Lt(a: ArithmeticExpr, b: ArithmeticExpr) extends Predicate

case class And(a: Ref, b: Ref) extends Predicate
case class Or(a: Ref, b: Ref) extends Predicate
case class Not(a: Ref) extends Predicate