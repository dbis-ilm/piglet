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

import dbis.test.TestTools._

import dbis.pig.PigCompiler._
import dbis.pig.op._
import dbis.pig.plan.{DataflowPlan, InvalidPlanException}
import dbis.pig.schema._
import org.scalatest.OptionValues._
import org.scalatest.{FlatSpec, Matchers}

class DataflowPlanSpec extends FlatSpec with Matchers {
  /*
  "The plan" should "contain all pipes" in {
    val op1 = Load("a", "file.csv")
    val op2 = Filter("b", "a", Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Dump("b")
    val plan = new DataflowPlan(List(op1, op2, op3))
    assert(plan.pipes == Map("a" -> Pipe("a", op1), "b" -> Pipe("b", op2)))
  }
  */

  "The plan" should "not contain duplicate pipes" in {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val op2 = Filter(Pipe("b"), Pipe("a"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Dump(Pipe("b"))
    val op4 = Filter(Pipe("b"), Pipe("a"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    intercept[InvalidPlanException] {
      new DataflowPlan(List(op1, op2, op3, op4))
    }
  }

  it should "check connectivity" in {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val op2 = Filter(Pipe("b"), Pipe("a"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Dump(Pipe("b"))
    val op4 = Filter(Pipe("c"), Pipe("a"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op5 = Dump(Pipe("c"))
    val plan = new DataflowPlan(List(op1, op2, op3, op4, op5))
    assert(plan.checkConnectivity)
  }

  it should "find disconnected operators" in {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val op2 = Filter(Pipe("b"), Pipe("a"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Dump(Pipe("b"))
    val op4 = Load(Pipe("c"), "input/file.csv")
    // val op5 = Dump(Pipe("c"))
    val plan = new DataflowPlan(List(op1, op2, op3, op4))
    assert(!plan.checkConnectivity)
  }

  it should "find sink operators" in {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val op2 = Filter(Pipe("b"), Pipe("a"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Dump(Pipe("b"))
    val op4 = Load(Pipe("c"), "input/file.csv")
    val op5 = Dump(Pipe("c"))
    val plan = new DataflowPlan(List(op1, op2, op3, op4, op5))
    assert(plan.sinkNodes == Set(op3, op5))
  }
  
  it should "find source operators" in {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val op2 = Filter(Pipe("b"), Pipe("a"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Dump(Pipe("b"))
    val op4 = Load(Pipe("c"), "input/file.csv")
    val op5 = Dump(Pipe("c"))
    val plan = new DataflowPlan(List(op1, op2, op3, op4, op5))
    assert(plan.sourceNodes == Set(op1, op4))
  }

  it should "return the operator producing the given relation" in {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val op2 = Filter(Pipe("b"), Pipe("a"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Load(Pipe("c"), "input/file.csv")
    val op4 = Filter(Pipe("d"), Pipe("c"), Lt(RefExpr(PositionalField(0)), RefExpr(Value("42"))))
    val plan = new DataflowPlan(List(op1, op2, op3, op4))
    plan.findOperatorForAlias("d") should equal (Some(op4))
    plan.findOperatorForAlias("b") should equal (Some(op2))
    plan.findOperatorForAlias("a") should equal (Some(op1))
    plan.findOperatorForAlias("x") should equal (None)
  }

  it should "eliminate register statements" in {
    val plan = new DataflowPlan(parseScript("""
         |register "myfile.jar";
         |a = load 'file.csv' as (f1:int, f2:chararray, f3:double);
         |b = filter a by f1 > 0;
         |""".stripMargin))
    plan.additionalJars.toList should equal (List("myfile.jar"))
    plan.operators.length should equal (2)
    plan.operators.filter(_.isInstanceOf[RegisterCmd]).length should equal (0)
  }

  it should "compute identical lineage signatures for two operators with the same plans" in {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val op2 = Filter(Pipe("b"), Pipe("a"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Grouping(Pipe("c"), Pipe("b"), GroupingExpression(List(PositionalField(0))))
    val plan1 = new DataflowPlan(List(op1, op2, op3))

    val op4 = Load(Pipe("a"), "input/file.csv")
    val op5 = Filter(Pipe("b"), Pipe("a"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op6 = Grouping(Pipe("c"), Pipe("b"), GroupingExpression(List(PositionalField(0))))
    val plan2 = new DataflowPlan(List(op4, op5, op6))
    assert(op3.lineageSignature == op6.lineageSignature)
  }

  it should "infer the schema for filter" in {
    val plan = new DataflowPlan(parseScript("""
        |a = load 'file.csv' as (f1:int, f2:chararray, f3:double);
        |b = filter a by f1 > 0;
        |""".stripMargin))
    val loadSchema = plan.operators(0).schema
    loadSchema should not be (None)
    val filterSchema = plan.operators(1).schema
    filterSchema should not be (None)
    loadSchema should equal (filterSchema)
    filterSchema match {
      case Some(s) => {
        s.field(0) should equal (Field("f1", Types.IntType))
        s.field(1) should equal (Field("f2", Types.CharArrayType))
        s.field(2) should equal (Field("f3", Types.DoubleType))
      }
      case None => fail()
    }
  }

  it should "infer the schema for a generate clause in foreach" in {
    val plan = new DataflowPlan(parseScript("""
        |a = load 'file.csv';
        |b = foreach a generate $0 as subject: chararray, $1 as predicate: chararray, $2 as object:bytearray;
        |""".stripMargin))
    val schema = plan.operators(1).schema
    schema match {
      case Some(s) => {
        s.field(0) should equal (Field("subject", Types.CharArrayType))
        s.field(1) should equal (Field("predicate", Types.CharArrayType))
        s.field(2) should equal (Field("object", Types.ByteArrayType))
      }
      case None => fail()
    }
  }

  it should "infer the schema for another generate clause in foreach" in {
    val plan = new DataflowPlan(parseScript("""
        |a = load 'file.csv';
        |b = foreach a generate $0+$1, $1 as f1: double, $2 as f3;
        |""".stripMargin))
    val schema = plan.operators(1).schema
    schema match {
      case Some(s) => {
        s.field(0) should equal (Field("", Types.DoubleType))
        s.field(1) should equal (Field("f1", Types.DoubleType))
        s.field(2) should equal (Field("f3", Types.ByteArrayType))
      }
      case None => fail()
    }
  }

  it should "infer the schema for a generate clause in foreach with type casts" in {
    val plan = new DataflowPlan(parseScript("""
        |a = load 'file.csv';
        |b = foreach a generate (int)$0, (tuple(int,int,float))$1 as f1;
        |""".stripMargin))
    val schema = plan.operators(1).schema
    schema match {
      case Some(s) => {
        s.field(0) should equal (Field("", Types.IntType))
        s.field(1) should equal (Field("f1", TupleType(Array(Field("", Types.IntType),
                                                                Field("", Types.IntType),
                                                                Field("", Types.FloatType)))))
      }
      case None => fail()
    }
  }

  it should "infer the schema for group by" in {
    val plan = new DataflowPlan(parseScript("""
        |a = load 'file.csv' as (f1: int, f2: double, f3:map[]);
        |b = group a by f1;
        |""".stripMargin))
    val schema = plan.operators(1).schema
    schema match {
      case Some(s) => {
        s.fields.length should equal (2)
        s.field(0) should equal(Field("group", Types.IntType))
        s.field(1) should equal(Field("a", BagType(TupleType(Array(Field("f1", Types.IntType),
                                                                      Field("f2", Types.DoubleType),
                                                                      Field("f3", MapType(Types.ByteArrayType))
        )))))
      }
      case None => fail()
    }
  }

  it should "infer the schema for group by on strings" in {
    val plan = new DataflowPlan(parseScript("""
                                              |a = load 'file.csv' as (f1: chararray, f2: double, f3:map[]);
                                              |b = group a by f1;
                                              |""".stripMargin))
    val schema = plan.operators(1).schema
    schema match {
      case Some(s) => {
        s.fields.length should equal (2)
        s.field(0) should equal(Field("group", Types.CharArrayType))
        s.field(1) should equal(Field("a", BagType(TupleType(Array(Field("f1", Types.CharArrayType),
          Field("f2", Types.DoubleType),
          Field("f3", MapType(Types.ByteArrayType))
        )))))
      }
      case None => fail()
    }
  }

  it should "infer the schema for group by on multiple keys" in {
    val plan = new DataflowPlan(parseScript("""
                                              |a = load 'file.csv' as (f1: chararray, f2: int, f3:map[]);
                                              |b = group a by (f1, f2);
                                              |""".stripMargin))
    val schema = plan.operators(1).schema
    schema match {
      case Some(s) => {
        s.fields.length should equal (2)
        s.field(0) should equal(Field("group", TupleType(Array(Field("f1", Types.CharArrayType), Field("f2", Types.IntType)))))
        s.field(1) should equal(Field("a", BagType(TupleType(Array(Field("f1", Types.CharArrayType),
          Field("f2", Types.IntType),
          Field("f3", MapType(Types.ByteArrayType))
        )))))
      }
      case None => fail()
    }
  }

  it should "infer the schema for group all" in {
    val plan = new DataflowPlan(parseScript("""
                                              |a = load 'file.csv' as (f1: int, f2: double, f3:map[]);
                                              |b = group a all;
                                              |""".stripMargin))
    val schema = plan.operators(1).schema
    schema match {
      case Some(s) => {
        s.fields.length should equal (2)
        s.field(0) should equal(Field("group", Types.CharArrayType))
        s.field(1) should equal(Field("a", BagType(TupleType(Array(Field("f1", Types.IntType),
          Field("f2", Types.DoubleType),
          Field("f3", MapType(Types.ByteArrayType))
        )))))
      }
      case None => fail()
    }
    plan.operators(1).checkSchemaConformance should be (true)
  }

  it should "detect an invalid schema for group by" in {
    val plan = new DataflowPlan(parseScript( """
                                              |a = load 'file.csv' as (f1: int, f2: double, f3:map[]);
                                              |b = group a by all;
                                              |""".stripMargin))
    an [SchemaException] should be thrownBy plan.checkSchemaConformance
  }

  it should "infer the schema for cross" in {
    val plan = new DataflowPlan(parseScript( """
                                               |a = load 'file.csv' as (f1:int, f2:chararray, f3:double);
                                               |b = load 'file.csv' as (f10:int, f11:double, f12:bytearray);
                                               |c = cross a, b;
                                               | """.stripMargin))
    val schema = plan.operators.last.schema
    schema match {
      case Some(s) => {
        s.fields.length should equal (6)
        s.field(0) should equal(Field("f1", Types.IntType, List("a")))
        s.field(1) should equal(Field("f2", Types.CharArrayType, List("a")))
        s.field(2) should equal(Field("f3", Types.DoubleType, List("a")))
        s.field(3) should equal(Field("f10", Types.IntType, List("b")))
        s.field(4) should equal(Field("f11", Types.DoubleType, List("b")))
        s.field(5) should equal(Field("f12", Types.ByteArrayType, List("b")))
      }
      case None => fail()
    }
  }

  it should "infer the schema for join" in {
    val plan = new DataflowPlan(parseScript("""
        |a = load 'file.csv' as (f1:int, f2:chararray, f3:double);
        |b = load 'file.csv' as (f10:int, f11:double, f12:bytearray);
        |c = join a by f1, b by f10;
        |""".stripMargin))
    val schema = plan.operators.last.schema
    schema match {
      case Some(s) => {
        s.fields.length should equal (6)
        s.field(0) should equal(Field("f1", Types.IntType, List("a")))
        s.field(1) should equal(Field("f2", Types.CharArrayType, List("a")))
        s.field(2) should equal(Field("f3", Types.DoubleType, List("a")))
        s.field(3) should equal(Field("f10", Types.IntType, List("b")))
        s.field(4) should equal(Field("f11", Types.DoubleType, List("b")))
        s.field(5) should equal(Field("f12", Types.ByteArrayType, List("b")))
      }
      case None => fail()
    }
  }

  it should "infer the schema for union with compatible relations" in {
    val plan = new DataflowPlan(parseScript("""
        |a = load 'file.csv' as (f1:int, f2:chararray, f3:double);
        |b = load 'file.csv' as (f1:int, f2:chararray, f3:double);
        |c = union a, b;
        |""".stripMargin))
    val schema = plan.operators.last.schema
    schema match {
      case Some(s) => {
        s.fields.length should equal (3)
        s.field(0) should equal(Field("f1", Types.IntType))
        s.field(1) should equal(Field("f2", Types.CharArrayType))
        s.field(2) should equal(Field("f3", Types.DoubleType))
      }
      case None => fail()
    }
  }

  it should "infer a null schema for union with relations of different sizes" in {
    val plan = new DataflowPlan(parseScript("""
         |a = load 'file.csv' as (f1:int, f2:chararray, f3:double, f4:int);
         |b = load 'file.csv' as (f1:int, f2:chararray, f3:double);
         |c = union a, b;
         |""".stripMargin))
    val schema = plan.operators.last.schema
    schema should equal (None)
  }

  it should "infer the schema for union with relations with different types" in {
    val plan = new DataflowPlan(parseScript("""
        |a = load 'file.csv' as (f1:int, f2:chararray, f3:float);
        |b = load 'file.csv' as (f11:double, f21:bytearray, f31:long);
        |c = union a, b;
        |""".stripMargin))
    val schema = plan.operators.last.schema
    schema match {
      case Some(s) => {
        s.fields.length should equal (3)
        s.field(0) should equal(Field("f1", Types.DoubleType))
        s.field(1) should equal(Field("f2", Types.CharArrayType))
        s.field(2) should equal(Field("f3", Types.FloatType))
      }
      case None => fail()
    }
  }

  it should "accept a filter statement with correct field names" in {
    val plan = new DataflowPlan(parseScript("""
        |a = load 'file.csv' as (f1:int, f2:chararray, f3:double);
        |b = filter a by f1 > 0;
        |""".stripMargin))
//    plan.checkSchemaConformance should equal (true)
    
    /* somehow the not does not work here
     * just let it check the conformance, if the exception is thrown, 
     * the test will fail anyway
     */
    plan.checkSchemaConformance
//    an [SchemaException] should not be thrownBy plan.checkSchemaConformance 
  }

  it should "reject a filter statement with incorrect field names" in {
    val plan = new DataflowPlan(parseScript("""
        |a = load 'file.csv' as (f1:int, f2:chararray, f3:double);
        |b = filter a by f0 > 0;
        |""".stripMargin))
//    plan.checkSchemaConformance should equal (false)
    an [SchemaException] should be thrownBy plan.checkSchemaConformance
  }

  it should "reject a filter statement with field names for unknown schema" in {
    val plan = new DataflowPlan(parseScript("""
        |a = load 'file.csv';
        |b = filter a by f0 > 0;
        |""".stripMargin))
//    plan.checkSchemaConformance should equal (false)
    an [SchemaException] should be thrownBy plan.checkSchemaConformance
  }

  it should "process a nested FOREACH statement with multiple statements" in {
    val ops = parseScript(
      """daily = load 'data.csv' as (exchange, symbol);
        |grpd  = group daily by exchange;
        |uniqcnt  = foreach grpd {
        |           sym      = daily.symbol;
        |           uniq_sym = distinct sym;
        |           generate group, COUNT(uniq_sym);
        |};""".stripMargin)
    val plan = new DataflowPlan(ops)
  }

  it should "check connectivity of a plan with a nested FOREACH" in {
    val ops = parseScript(
      """daily = load 'data.csv' as (exchange, symbol);
        |grpd  = group daily by exchange;
        |uniqcnt  = foreach grpd {
        |           sym      = daily.symbol;
        |           uniq_sym = distinct sym;
        |           generate group, COUNT(uniq_sym);
        |};
        |dump uniqcnt;""".stripMargin)
    val plan = new DataflowPlan(ops)
    plan.checkConnectivity should be (true)
  }

  it should "detect an invalid plan with a nested FOREACH" in {
    val ops = parseScript(
      """daily = load 'data.csv' as (exchange, symbol);
        |grpd  = group daily by exchange;
        |uniqcnt  = foreach grpd {
        |           sym      = daily2.symbol;
        |           uniq_sym = distinct sym;
        |           generate group, COUNT(uniq_sym);
        |};""".stripMargin)
    an [SchemaException] should be thrownBy new DataflowPlan(ops)
  }

  it should "detect another invalid plan with a nested FOREACH" in {
    val ops = parseScript(
      """daily = load 'data.csv' as (exchange, symbol);
        |grpd  = group daily by exchange;
        |uniqcnt  = foreach grpd {
        |           sym      = daily.symbol;
        |           uniq_sym = distinct sym2;
        |           generate group, COUNT(uniq_sym2);
        |};""".stripMargin)
    an [InvalidPlanException] should be thrownBy new DataflowPlan(ops)
  }

  it should "detect a third invalid plan with a nested FOREACH" in {
    val ops = parseScript(
      """daily = load 'data.csv' as (exchange, symbol);
        |grpd  = group daily by exchange;
        |uniqcnt  = foreach grpd {
        |           sym      = daily.symbol;
        |           uniq_sym = distinct sym;
        |           generate group, COUNT(uniq_sym2);
        |};""".stripMargin)
    val plan = new DataflowPlan(ops)
    an [SchemaException] should be thrownBy plan.checkSchemaConformance
  }

  it should "detect a fourth invalid plan with a nested FOREACH" in {
    val ops = parseScript(
      """daily = load 'data.csv' as (exchange, symbol);
        |grpd  = group daily by exchange;
        |uniqcnt  = foreach grpd {
        |           sym      = daily.symbol;
        |           uniq_sym = distinct sym;
        |};""".stripMargin)
    an [InvalidPlanException] should be thrownBy new DataflowPlan(ops)
  }

  it should "be consistent after adding a new operator using insertAfter" in {
    val plan = new DataflowPlan(parseScript("""
         |a = load 'file.csv';
         |b = filter a by $0 > 0;
         |""".stripMargin))
    var op = plan.findOperatorForAlias("a").value
    val d = Distinct(Pipe("d"),Pipe("a"))
    val newPlan = plan.insertAfter(op, d)

    op = newPlan.findOperatorForAlias("a").value
    val b = newPlan.findOperatorForAlias("b").value
    op.outputs should have size 1
    val output = op.outputs.headOption.value
    
    output.consumer should contain only(b, d)
  }

  it should "be consistent after exchanging two operators" in {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val spec1 = OrderBySpec(PositionalField(1), OrderByDirection.AscendingOrder)
    val op2 = OrderBy(Pipe("b"), Pipe("a"), List(spec1))
    val spec2 = OrderBySpec(NamedField("a"), OrderByDirection.DescendingOrder)
    val op3 = OrderBy(Pipe("c"), Pipe("b"), List(spec2))
    val op4 = Dump(Pipe("c"))

    val plan = new DataflowPlan(List(op1, op2, op3, op4))
    val newPlan = plan.swap(op2, op3)

    newPlan.sourceNodes.headOption.value.outputs.head.consumer should contain only(op3)
    val sinkInput = newPlan.sinkNodes.headOption.value.inputs.headOption.value
    sinkInput.name shouldBe "c"
    sinkInput.producer shouldBe op2
  }

  it should "be consistent after removing an operator" in {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val predicate = Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))
    val op2 = Filter(Pipe("b"), Pipe("a"), predicate)
    val op3 = Dump(Pipe("b"))

    val plan = new DataflowPlan(List(op1, op2, op3))
    val newPlan = plan.remove(op2)

    newPlan.sinkNodes.headOption.value.inputs should contain only Pipe("a", op1)
    newPlan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op3
  }

  it should "allow removing a node and its predecessors" in {
    val op1 = Load(Pipe("a"), "blablabla.csv")
    val spec1 = OrderBySpec(PositionalField(1), OrderByDirection.AscendingOrder)
    val op2 = OrderBy(Pipe("b"), Pipe("a"), List(spec1))
    val spec2 = OrderBySpec(NamedField("a"), OrderByDirection.DescendingOrder)
    val op3 = OrderBy(Pipe("lalala"), Pipe("b"), List(spec2))
    val op4 = Dump(Pipe("lalala"))

    val plan = new DataflowPlan(List(op1, op2, op3, op4)).remove(op3, removePredecessors = true)
    plan.operators should contain only(op4)
  }

  it should "return an empty DataflowPlan when the last operator in the plan is removed" in {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val plan = new DataflowPlan(List(op1))
    val newPlan = plan.remove(op1)
    newPlan.operators shouldBe empty
  }

  it should "construct pipes for SPLIT INTO" in {
    val plan = new DataflowPlan(parseScript(s"""
      |a = LOAD 'file' AS (x, y);
      |SPLIT a INTO b IF x < 100, c IF x >= 100;
      |STORE b INTO 'res1.data';
      |STORE c INTO 'res2.data';""".stripMargin))
    plan.findOperatorForAlias("b") should not be empty
    plan.sourceNodes.headOption.value.outputs.headOption.value.consumer.headOption.value.outputs should have length 2
    assert(plan.checkConnectivity)
  }

  it should "reject multiple pipes with the same name" in {
    val op = Load(Pipe("a"), "input/file.csv")
    an [InvalidPlanException] should be thrownBy {
      op.outputs = List(Pipe("b"), Pipe("b"))
    }
  }

  it should "propage set parameters to the operators" in {
    val plan = new DataflowPlan(parseScript(s"""
       |A = LOAD 'file' AS (x, y);
       |SET parallelismHint 5;
       |B = FILTER A BY x > 100;
       |C = FILTER B BY y > 100;
       |SET parallelismHint 3;
       |SET some "thing";
       |D = FILTER C by x < 100;""".stripMargin))
    val opB = plan.findOperatorForAlias("B")
    val opC = plan.findOperatorForAlias("C")
    val opD = plan.findOperatorForAlias("D")
    opB.get.configParams should contain key ("parallelismHint")
    opB.get.configParams("parallelismHint") should be (Value(5))
    opC.get.configParams should contain key ("parallelismHint")
    opC.get.configParams("parallelismHint") should be (Value(5))
    opC.get.configParams should not contain key ("some")
    opD.get.configParams should contain key ("parallelismHint")
    opD.get.configParams should contain key ("some")
    opD.get.configParams("parallelismHint") should be (Value(3))
    opD.get.configParams("some") should be (Value("\"thing\""))
  }

  it should "find an alias in a plan" in {
    val plan = new DataflowPlan(parseScript(
      """triples = LOAD 'file' AS (sub, pred, obj);
        |stmts = GROUP triples BY sub;
      """.stripMargin))
    val op = plan.findOperatorForAlias("stmts")
    op shouldBe defined
  }
}
