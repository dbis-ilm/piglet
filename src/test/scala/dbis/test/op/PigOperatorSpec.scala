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
package dbis.test.op

import dbis.pig.PigCompiler._
import dbis.pig.op.Foreach
import dbis.pig.plan._
import dbis.pig.schema._
import org.scalatest.OptionValues._
import org.scalatest.{FlatSpec, Matchers}

class PigOperatorSpec extends FlatSpec with Matchers {
  /*
  "An operator" should "allow updating its output relation name if it has no outputs" in {
    val op = Limit(Pipe("b"), Pipe("a"), 5)
    op.outputs = List(Pipe("b"))
  }

  it should "not allow updating its output relation name if not all outputs read from the new name" in {
    val op = Limit(Pipe("b"), Pipe("a"), 5)
    val op2 = Limit(Pipe("c"), Pipe("b"), 3)
    op.outputs = List(op2)
    intercept[IllegalArgumentException] {
      op.output = Some("d")
    }
  }

  it should "not allow unsetting its output relation if it has outputs" in {
    val op = Limit(Pipe("b"), Pipe("a"), 5)
    val op2 = Limit(Pipe("c"), Pipe("b"), 3)
    op.output = Some(Pipe("b"))
    op.outputs = List(op2)
    intercept[IllegalStateException] {
      op.output = None
    }
  }

  it should "not allow updating its outputs if it doesn't return a relation" in {
    val op = Store(Pipe("a"), "foo.csv")
    val op2 = Limit(Pipe("a"), Pipe("b"), 5)
    intercept[IllegalStateException] {
      op.outputs = List(op2)
    }
  }

  it should "allow updating its outputs if the new ones read from it" in {
    val op = Limit(Pipe("b"), Pipe("a"), 5)
    op.output = Some(Pipe("b"))
    val op2 = Limit(Pipe("c"), Pipe("b"), 3)
    op.outputs = List(op2)
  }

  it should "not allow updating its outputs if the new ones don't read from it" in {
    val op = Limit(Pipe("b"), Pipe("a"), 5)
    op.output = Some(Pipe("b"))
    val op2 = Limit("d", Pipe("c"), 3)
    intercept[IllegalArgumentException] {
      op.outputs = List(op2)
    }
  }

  it should "allow unsetting its outputs" in {
    val op = Limit(Pipe("b"), Pipe("a"), 5)
    op.output = Some(Pipe("b"))
    val op2 = Limit(Pipe("c"), Pipe("b"), 3)
    op.outputs = List(op2)
    op.outputs = List()
  }
*/
  "The FOREACH operator" should "allow to check for FLATTEN expression" in {
    val plan = new DataflowPlan(parseScript("b = load 'data'; a = foreach b generate $0, flatten($1);"))
    val ops = plan.operators
    val foreachOp: Foreach = ops(1).asInstanceOf[Foreach]
    val schema1 = BagType(TupleType(Array(Field("a", Types.IntType),
                                          Field("t", TupleType(Array(
                                              Field("f1", Types.IntType),
                                              Field("f2", Types.IntType)))))))
    ops.head.schema = Some(Schema(schema1))

    foreachOp.containsFlatten(false) should be (true)
    foreachOp.containsFlatten(true) should be (false)


    val schema2 = BagType(TupleType(Array(Field("a", Types.IntType),
                                          Field("b", BagType(TupleType(Array(
                                                                  Field("f1", Types.IntType))))))))
    ops.head.schema = Some(Schema(schema2))
    foreachOp.containsFlatten(false) should be (true)
    foreachOp.containsFlatten(true) should be (true)


  }

  "Arity" should "be the number of consumers for all outputs" in {
    val plan = new DataflowPlan(parseScript( s"""
                                                |a = LOAD 'file' AS (x, y);
                                                |SPLIT a INTO b IF x < 100, c IF x >= 100;
                                                |STORE b INTO 'res1.data';
                                                |STORE c INTO 'res2.data';
                                                |d = FILTER b by x < 50;""".stripMargin))
    val splitOp = plan.findOperatorForAlias("b")
    splitOp.value.arity shouldBe 3
  }
}
