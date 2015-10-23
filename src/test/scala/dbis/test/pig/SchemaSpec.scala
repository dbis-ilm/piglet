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

import dbis.pig.op.NamedField
import dbis.pig.plan.DataflowPlan
import dbis.pig.schema._
import org.scalatest.{Matchers, OptionValues, FlatSpec}
import dbis.pig.PigCompiler._

class SchemaSpec extends FlatSpec with OptionValues with Matchers {

  "The schema" should "contain f1, f2" in {
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.DoubleType),
                                                              Field("f2", Types.CharArrayType)))))
    assert(schema.indexOfField("f1") == 0)
    assert(schema.indexOfField("f2") == 1)
    assert(schema.field(0) == Field("f1", Types.DoubleType))
    assert(schema.field(1) == Field("f2", Types.CharArrayType))
  }

  "A fields toString" should "contain its lineage information after a JOIN" in {
    val plan = new DataflowPlan(parseScript(
      """
        | A = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (a1:int,a2:int);
        | B = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (b1:int,b2:int);
        | C = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (c1:int,c2:int);
        | X = JOIN A BY a1, B BY b1;
        | Y = JOIN X BY a1, C BY c1;

      """.stripMargin))
    plan.findOperatorForAlias("Y").value.schemaToString shouldBe
      "Y: {X::A::a1: int, X::A::a2: int, X::B::b1: int, X::B::b2: int, C::c1: int, C::c2: int}"
  }

  it should "contain its lineage information after a CROSS" in {
    val plan = new DataflowPlan(parseScript(
      """
        | A = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (a1:int,a2:int);
        | B = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (b1:int,b2:int);
        | C = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (c1:int,c2:int);
        | X = CROSS A , B;
        | Y = CROSS X , C;

      """.stripMargin))
    plan.findOperatorForAlias("Y").value.schemaToString shouldBe
      "Y: {X::A::a1: int, X::A::a2: int, X::B::b1: int, X::B::b2: int, C::c1: int, C::c2: int}"
  }

  "IndexOfField" should "return the correct index if name and lineage information are unambiguous information" in {
    val plan = new DataflowPlan(parseScript(
      """
        | A = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (a1:int,a2:int);
        | B = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (a1:int,a2:int);
        | C = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (c1:int,c2:int);
        | X = CROSS A , B;
        | Y = CROSS X , C;

      """.stripMargin))
    val s = plan.findOperatorForAlias("Y").value.schema.value
    s.indexOfField(NamedField("a1", List("X", "A"))) shouldBe 0
    s.indexOfField(NamedField("c1", List("C"))) shouldBe 4
  }

  it should "return the correct index if the field name is unambiguous" in {
    val plan = new DataflowPlan(parseScript(
      """
        | A = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (a1:int,a2:int);
        | B = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (b1:int,b2:int);
        | C = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (c1:int,c2:int);
        | X = CROSS A , B;
        | Y = CROSS X , C;

      """.stripMargin))
    val s = plan.findOperatorForAlias("Y").value.schema.value
    s.indexOfField(NamedField("b1", List())) shouldBe 2
    s.indexOfField(NamedField("c1", List())) shouldBe 4
  }

  it should "return the -1 if the field name but not the lineage information matches" in {
    val plan = new DataflowPlan(parseScript(
      """
        | A = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (a1:int,a2:int);
        | B = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (b1:int,b2:int);
        | C = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (c1:int,c2:int);
        | X = CROSS A , B;
        | Y = CROSS X , C;

      """.stripMargin))
    val s = plan.findOperatorForAlias("Y").value.schema.value
    s.indexOfField(NamedField("b1", List("C"))) shouldBe -1
  }
}
