package dbis.test.pig

/**
 * Created by kai on 01.04.15.
 */

import dbis.pig.PigCompiler._
import dbis.pig._
import org.scalatest.{Matchers, FlatSpec}

class DataflowPlanSpec extends FlatSpec with Matchers {
  "The plan" should "contain all pipes" in {
    val op1 = Load("a", "file.csv")
    val op2 = Filter("b", "a", Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val op3 = Dump("b")
    val plan = new DataflowPlan(List(op1, op2, op3))
    assert(plan.pipes == Map("a" -> Pipe("a", op1), "b" -> Pipe("b", op2)))
  }

  it should "not contain duplicate pipes" in {
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
        |a = load "file.csv" as (f1:int, f2:chararray, f3:double);
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
        |a = load "file.csv";
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
        |a = load "file.csv";
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
        |a = load "file.csv";
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
        |a = load "file.csv" as (f1: int, f2: double, f3:map[]);
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
        |a = load "file.csv" as (f1:int, f2:chararray, f3:double);
        |b = load "file.csv" as (f10:int, f11:double, f12:bytearray);
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
        |a = load "file.csv" as (f1:int, f2:chararray, f3:double);
        |b = load "file.csv" as (f1:int, f2:chararray, f3:double);
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
         |a = load "file.csv" as (f1:int, f2:chararray, f3:double, f4:int);
         |b = load "file.csv" as (f1:int, f2:chararray, f3:double);
         |c = union a, b;
         |""".stripMargin))
    val schema = plan.operators.last.schema
    schema should equal (None)
  }

  it should "infer the schema for union with relations with different types" in {
    val plan = new DataflowPlan(parseScript("""
        |a = load "file.csv" as (f1:int, f2:chararray, f3:float);
        |b = load "file.csv" as (f11:double, f21:bytearray, f31:long);
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
        |a = load "file.csv" as (f1:int, f2:chararray, f3:double);
        |b = filter a by f1 > 0;
        |""".stripMargin))
    plan.checkSchemaConformance should equal (true)
  }

  it should "reject a filter statement with correct field names" in {
    val plan = new DataflowPlan(parseScript("""
        |a = load "file.csv" as (f1:int, f2:chararray, f3:double);
        |b = filter a by f0 > 0;
        |""".stripMargin))
    plan.checkSchemaConformance should equal (false)
  }

  it should "reject a filter statement with field names for unknown schema" in {
    val plan = new DataflowPlan(parseScript("""
        |a = load "file.csv";
        |b = filter a by f0 > 0;
        |""".stripMargin))
    plan.checkSchemaConformance should equal (false)
  }
}
