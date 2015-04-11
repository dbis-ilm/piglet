package dbis.pig

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