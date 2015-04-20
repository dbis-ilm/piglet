package dbis.test.pig

import dbis.pig._
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by kai on 19.04.15.
 */
class ExprSpec extends FlatSpec with Matchers {
  "The expression traversal" should "check for schema conformance" in {
    val expr = Lt(Div(RefExpr(Value(10)), RefExpr(PositionalField(0))),
      RefExpr(NamedField("f1")))
    val schema = new Schema(BagType("s", TupleType("t", Array(Field("f1", Types.DoubleType),
      Field("f2", Types.IntType)))))
    expr.traverse(schema, Expr.checkExpressionConformance) should be (true)
  }

  it should "find named fields" in {
    val expr = Mult(RefExpr(PositionalField(0)),
              Add(RefExpr(Value(0)), RefExpr(NamedField("f1"))))
    println("---------------------------")
    expr.traverse(null, Expr.containsNoNamedFields) should be (false)
  }
}
