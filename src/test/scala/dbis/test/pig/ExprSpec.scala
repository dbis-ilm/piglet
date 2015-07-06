/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dbis.test.pig

import dbis.pig._
import dbis.pig.op._
import dbis.pig.schema._
import org.scalatest.{FlatSpec, Matchers}

class ExprSpec extends FlatSpec with Matchers {
  "The expression traversal" should "check for schema conformance" in {
    val expr = Lt(Div(RefExpr(Value(10)), RefExpr(PositionalField(0))),
      RefExpr(NamedField("f1")))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.DoubleType),
      Field("f2", Types.IntType)))))
    expr.traverseAnd(schema, Expr.checkExpressionConformance) should be (true)
  }

  it should "find named fields" in {
    val expr = Mult(RefExpr(PositionalField(0)),
              Add(RefExpr(Value(0)), RefExpr(NamedField("f1"))))
    expr.traverseAnd(null, Expr.containsNoNamedFields) should be (false)
  }

  it should "check for flatten in an expression correctly" in {
    val expr1 = FlattenExpr(RefExpr(PositionalField(0)))
    val expr2 = Add(RefExpr(PositionalField(0)), RefExpr(Value("10")))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.DoubleType)))))
    expr1.traverseOr(schema, Expr.containsFlatten) should be (true)
    expr2.traverseOr(schema, Expr.containsFlatten) should be (false)
  }

  it should "check for flatten on a bag in an expression" in {
    val expr1 = FlattenExpr(RefExpr(PositionalField(0)))
    val expr2 = Add(RefExpr(PositionalField(0)), RefExpr(Value("10")))
    val schema1 = new Schema(BagType(TupleType(Array(Field("f1", Types.DoubleType)))))
    val schema2 = new Schema(BagType(TupleType(Array(Field("f1", BagType(TupleType(Array(Field("ff1", Types.IntType)))))))))
    expr1.traverseOr(schema1, Expr.containsFlattenOnBag) should be (false)
    expr2.traverseOr(schema1, Expr.containsFlattenOnBag) should be (false)
    expr1.traverseOr(schema2, Expr.containsFlattenOnBag) should be (true)
    expr2.traverseOr(schema2, Expr.containsFlattenOnBag) should be (false)
  }

  "An expression" should "return the correct result type for +" in {
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.DoubleType),
                                                          Field("f2", Types.IntType),
                                                          Field("f3", Types.ByteArrayType)
    ))))

    val e1 = Add(RefExpr(NamedField("f1")), RefExpr(NamedField("f2")))
    e1.resultType(Some(schema)) should be (("", Types.DoubleType))
  }

  it should "return the correct result type for casts" in {
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.DoubleType),
      Field("f2", Types.IntType),
      Field("f3", Types.ByteArrayType)
    ))))

    val e1 = CastExpr(Types.IntType, RefExpr(PositionalField(0)))
    e1.resultType(Some(schema)) should be (("", Types.IntType))

    val tupleType = TupleType(Array(Field("", Types.IntType), Field("", Types.DoubleType)))
    val e2 = CastExpr(tupleType, RefExpr(NamedField("f3")))
    e2.resultType(Some(schema)) should be (("f3", tupleType))
  }
}
