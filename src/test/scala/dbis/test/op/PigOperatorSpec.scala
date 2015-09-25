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
import java.net.URI

class PigOperatorSpec extends FlatSpec with Matchers {
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

  "PigOperators" should "return useful lineage strings" in {
    val plan = new DataflowPlan(parseScript(s"""
         |A = LOAD 'file' AS (x, y, z);
         |B = FILTER A BY x > 0;
         |C = SAMPLE B 0.1;
         |D = GROUP C BY x;
         |E = FOREACH D GENERATE group, COUNT(C) AS cnt;
         |F = DISTINCT E;
         |G = ORDER F BY cnt;
         |H = LIMIT G 20;
         |""".stripMargin))
    val plan2 = new DataflowPlan(parseScript(s"""
         |A = LOAD 'file2' AS (x, y, z);
         |B = FILTER A BY x < 0;
         |CC = SAMPLE B 0.1;
         |DD = GROUP CC BY x;
         |EE = FOREACH DD GENERATE group, COUNT(C) AS counter;
         |F = DISTINCT EE;
         |G = ORDER F BY counter;
         |H = LIMIT G 25;
         |""".stripMargin))
    plan.findOperatorForAlias("A").get.lineageString should not be equal (plan2.findOperatorForAlias("A").get.lineageString)
    plan.findOperatorForAlias("B").get.lineageString should not be equal (plan2.findOperatorForAlias("A").get.lineageString)
    plan.findOperatorForAlias("C").get.lineageString should not be equal (plan2.findOperatorForAlias("CC").get.lineageString)
    plan.findOperatorForAlias("D").get.lineageString should not be equal (plan2.findOperatorForAlias("DD").get.lineageString)
    plan.findOperatorForAlias("E").get.lineageString should not be equal (plan2.findOperatorForAlias("EE").get.lineageString)
    plan.findOperatorForAlias("F").get.lineageString should not be equal (plan2.findOperatorForAlias("F").get.lineageString)
    plan.findOperatorForAlias("G").get.lineageString should not be equal (plan2.findOperatorForAlias("G").get.lineageString)
    plan.findOperatorForAlias("H").get.lineageString should not be equal (plan2.findOperatorForAlias("H").get.lineageString)
  }
}
