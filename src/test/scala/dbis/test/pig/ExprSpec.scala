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
    expr.traverse(null, Expr.containsNoNamedFields) should be (false)
  }

  "An expression" should "return the correct result type" in {
    val schema = new Schema(BagType("", TupleType("", Array(Field("f1", Types.DoubleType),
                                                          Field("f2", Types.IntType),
                                                          Field("f3", Types.ByteArrayType)
    ))))

    val e1 = Add(RefExpr(NamedField("f1")), RefExpr(NamedField("f2")))
    e1.resultType(Some(schema)) should be (("", Types.DoubleType))

    val e2 = CastExpr(Types.IntType, RefExpr(PositionalField(0)))
    e2.resultType(Some(schema)) should be (("", Types.IntType))

    val tupleType = TupleType("", Array(Field("", Types.IntType), Field("", Types.DoubleType)))
    val e3 = CastExpr(tupleType, RefExpr(NamedField("f3")))
    e3.resultType(Some(schema)) should be (("f3", tupleType))

  }
}
