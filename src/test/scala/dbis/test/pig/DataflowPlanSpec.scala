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

import dbis.pig.PigCompiler._
import dbis.pig.op._
import dbis.pig.plan.{Pipe, DataflowPlan, InvalidPlanException}
import dbis.pig.schema._
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
    val op1 = Load("a", "file.csv")
    val op2 = Filter("b", "a", Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Dump("b")
    val op4 = Filter("b", "a", Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    intercept[InvalidPlanException] {
      new DataflowPlan(List(op1, op2, op3, op4))
    }
  }

  it should "check connectivity" in {
    val op1 = Load("a", "file.csv")
    val op2 = Filter("b", "a", Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Dump("b")
    val op4 = Filter("c", "a", Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op5 = Dump("c")
    val plan = new DataflowPlan(List(op1, op2, op3, op4, op5))
    assert(plan.checkConnectivity)
  }

  it should "find disconnected operators" in {
    val op1 = Load("a", "file.csv")
    val op2 = Filter("b", "a", Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Dump("b")
    val op4 = Load("c", "file.csv")
    val op5 = Dump("c")
    val plan = new DataflowPlan(List(op1, op2, op3, op4, op5))
    assert(!plan.checkConnectivity)
  }

  it should "find sink operators" in {
    val op1 = Load("a", "file.csv")
    val op2 = Filter("b", "a", Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Dump("b")
    val op4 = Load("c", "file.csv")
    val op5 = Dump("c")
    val plan = new DataflowPlan(List(op1, op2, op3, op4, op5))
    assert(plan.sinkNodes == Set(op3, op5))
  }
  
  it should "find source operators" in {
    val op1 = Load("a", "file.csv")
    val op2 = Filter("b", "a", Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Dump("b")
    val op4 = Load("c", "file.csv")
    val op5 = Dump("c")
    val plan = new DataflowPlan(List(op1, op2, op3, op4, op5))
    assert(plan.sourceNodes == Set(op1, op4))
  }

  it should "return the operator producing the given relation" in {
    val op1 = Load("a", "file.csv")
    val op2 = Filter("b", "a", Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Load("c", "file.csv")
    val op4 = Filter("d", "c", Lt(RefExpr(PositionalField(0)), RefExpr(Value("42"))))
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
    plan.operators.filter(_.isInstanceOf[Register]).length should equal (0)
  }

  it should "compute identical lineage signatures for two operators with the same plans" in {
    val op1 = Load("a", "file.csv")
    val op2 = Filter("b", "a", Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Grouping("c", "b", GroupingExpression(List(PositionalField(0))))
    val plan1 = new DataflowPlan(List(op1, op2, op3))

    val op4 = Load("a", "file.csv")
    val op5 = Filter("b", "a", Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op6 = Grouping("c", "b", GroupingExpression(List(PositionalField(0))))
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
        s.field(1) should equal (Field("f1", TupleType("", Array(Field("", Types.IntType),
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
        s.field(1) should equal(Field("a", BagType("", TupleType("", Array(Field("f1", Types.IntType),
                                                                      Field("f2", Types.DoubleType),
                                                                      Field("f3", MapType("", Types.ByteArrayType))
        )))))
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
        s.field(0) should equal(Field("f1", Types.IntType))
        s.field(1) should equal(Field("f2", Types.CharArrayType))
        s.field(2) should equal(Field("f3", Types.DoubleType))
        s.field(3) should equal(Field("f10", Types.IntType))
        s.field(4) should equal(Field("f11", Types.DoubleType))
        s.field(5) should equal(Field("f12", Types.ByteArrayType))
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

  it should "be consistent after adding a new operator using insertAfter" in {
    val plan = new DataflowPlan(parseScript("""
         |a = load 'file.csv';
         |b = filter a by $0 > 0;
         |""".stripMargin))
    
    val ops = plan.findOperator(o => o.outPipeName == "a")
    withClue("did not find operator a") {ops.size should be (1)}
    
    val op = ops.head
    
    op.outPipeName should be ("a")
    
    val newOp = Distinct("c", "a")
    
    // FIXME: PigOperator has many different names
    /* there is initialOutPipeName, outPipeName, and output (and outputs -> the child operators) 
     * we need to make this consistent
     * (same for inputs) 
     */
//    newOp.outPipeName shouldBe "c"
    
    plan.insertAfter(op, newOp)
    
    plan.operators should contain (newOp)
    
    val fs = plan.findOperator(_ == newOp)
    withClue("did not find operator c") {fs.size shouldBe 1}
    
    val f = fs.head
    withClue("unexpected number of inputs") {f.inputs.size shouldBe 1}
    f should be (newOp)
    
    withClue("only input should be a") {f.inputs.head.name shouldBe "a"}
    withClue("only input should be a op") {f.inputs.head.producer shouldBe op}
    
    // check outputs of a to be b and c
    val b = plan.findOperatorForAlias("b").get

    op.outputs contains only(b, newOp)
  }

  it should "be consistent after swapping two operators" in {

  }
  
  it should "be consistent after replacing an operator" in {
    val plan = new DataflowPlan(parseScript("""
         |a = load 'file.csv';
         |b = filter a by $0 > 0;
         |c = distinct b;
         |""".stripMargin))
    
    val ops = plan.findOperator(o => o.outPipeName == "b")
    withClue("did not find operator a") {ops.size should be (1)}
    
    val b = ops.head
    
    val newB = Distinct("a", "b")
    
    plan.replace(b, newB)
    
    plan.operators should contain (newB)
    plan.operators should not contain (b)
    
    withClue("old inputs: ") {b.inputs shouldBe empty}
    withClue("old ouputs: ") {b.outputs shouldBe empty}
    // the following two would need to reassign a val --> make it a var?
//    withClue("initial in pipe names: ") {b.initialInPipeNames shouldBe empty}
//    withClue("initial out pipe names: ") {b.initialOutPipeName shouldBe null}
    b.output shouldBe None
    
    
    val fs = plan.findOperator(_ == newB)
    withClue("did not find operator c") {fs.size shouldBe 1}
    
    val f = fs.head
    withClue("unexpected number of inputs") {f.inputs.size shouldBe 1}
    f should be (newB)
    withClue("only input should be a") {f.inputs.head.name shouldBe "a"}
    
    val a = plan.findOperatorForAlias("a").get
    val c = plan.findOperatorForAlias("c").get
    
    withClue("new op's outputs") {newB.outputs should contain only c}
    withClue("new op's inputs") {newB.inputs.map(_.producer) should contain only a}
    
    withClue("new op's succeccssor's input") {c.inputs should contain only Pipe(newB.initialInPipeName, newB)}
    

  }
  
  it should "be consistent after removing an operator" in {
    val plan = new DataflowPlan(parseScript("""
         |a = load 'file.csv';
         |b = filter a by $0 > 0;
         |c = distinct b;
         |""".stripMargin))
    
    val b = plan.findOperatorForAlias("b").get
    
    plan.remove(b)
    
    withClue("operators: ") {plan.operators should not contain b}

    val a = plan.findOperatorForAlias("a").get
    withClue("a outputs: ") {a.outputs should not contain b}
    
    val c = plan.findOperatorForAlias("c").get
    withClue("c inputs: ") {c.inputs.map(_.producer) should not contain b}
  }

  it should "correctly assign inputs and outputs" in {
    val op1 = Load("a", "file.csv")
    val predicate = Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))
    val op2 = Filter("b", "a", predicate)
    val op3 = Dump("b")
    val op4 = OrderBy("c", "b", List())
    val op5 = Dump("c")
    val ops = List[PigOperator](op1, op2, op3, op4, op5)
    for(op <- ops) {
      op.inputs shouldBe empty
      op.outputs shouldBe empty
    }
    val plan = new DataflowPlan(ops)

    op1.output shouldBe Some("a")
    op1.outputs should contain only (op2)

    op2.inputs should contain only (Pipe("a", op1))
    op2.output shouldBe Some("b")
    op2.outputs should contain allOf(op3, op4)

    op3.inputs should contain only (Pipe("b", op2))
    op3.output shouldBe None
    op3.outputs shouldBe empty

    op4.inputs should contain only (Pipe("b", op2))
    op4.output shouldBe Some("c")
    op4.outputs should contain only(op5)

    op5.inputs should contain only (Pipe("c", op4))
    op5.output shouldBe None
    op5.outputs shouldBe empty
  }
  
}
