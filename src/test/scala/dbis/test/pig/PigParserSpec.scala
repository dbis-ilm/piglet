/**
 * Created by kai on 31.03.15.
 */
package dbis.test.pig

import dbis.pig._
import dbis.pig.PigCompiler._
import org.scalatest.FlatSpec

class PigParserSpec extends FlatSpec {
  "The parser" should "parse a simple load statement" in  {
    assert(parseScript("""a = load "file.csv";""") == List(Load("a", "file.csv")))
  }

  it should "parse also a case insensitive load statement" in  {
    assert(parseScript("""a = LOAD "file.csv";""") == List(Load("a", "file.csv")))
  }

  it should "parse also a load statement with the using clause" in  {
    assert(parseScript("""a = LOAD "file.data" using PigStorage("\t");""") ==
      List(Load("a", "file.data", None, "PigStorage", List(""""\t""""))))
    assert(parseScript("""a = LOAD "file.n3" using RDFFileStorage();""") ==
      List(Load("a", "file.n3", None, "RDFFileStorage")))
  }

  it should "parse a load statement with typed schema specification" in {
    val schema = BagType("", TupleType("", Array(Field("a", Types.IntType),
                                                Field("b", Types.CharArrayType),
                                                Field("c", Types.DoubleType))))
    assert(parseScript("""a = load "file.csv" as (a:int, b:chararray, c:double); """) ==
      List(Load("a", "file.csv", Some(Schema(schema)))))
  }

  it should "parse a load statement with complex typed schema specification" in {
    val schema = BagType("", TupleType("", Array(Field("a", Types.IntType),
      Field("t", TupleType("", Array(Field("f1", Types.IntType), Field("f2", Types.IntType)))),
      Field("b", BagType("", TupleType("t2", Array(Field("f3", Types.DoubleType), Field("f4", Types.DoubleType))))))))
    assert(parseScript("""a = load "file.csv" as (a:int, t:tuple(f1: int, f2:int), b:{t2:tuple(f3:double, f4:double)}); """) ==
      List(Load("a", "file.csv", Some(Schema(schema)))))
  }

  it should "parse another load statement with complex typed schema specification" in {
    val schema = BagType("", TupleType("", Array(Field("a", Types.IntType),
      Field("m1", MapType("", Types.CharArrayType)),
      Field("m2", MapType("", TupleType("", Array(Field("f1", Types.IntType), Field("f2", Types.IntType))))),
      Field("m3", MapType("", Types.ByteArrayType)))))
    assert(parseScript("""a = load "file.csv" as (a:int, m1:map[chararray], m2:[(f1: int, f2:int)], m3:[]); """) ==
      List(Load("a", "file.csv", Some(Schema(schema)))))
  }

  it should "parse a load statement with typed schema specification and using clause" in {
    val schema = BagType("", TupleType("", Array(Field("a", Types.IntType),
      Field("b", Types.CharArrayType),
      Field("c", Types.DoubleType))))
    assert(parseScript("""a = load "file.data" using PigStorage() as (a:int, b:chararray, c:double); """) ==
      List(Load("a", "file.data", Some(Schema(schema)), "PigStorage")))
  }

  it should "parse a load statement with untyped schema specification" in {
    val schema = BagType("", TupleType("", Array(Field("a", Types.ByteArrayType),
      Field("b", Types.ByteArrayType),
      Field("c", Types.ByteArrayType))))
    assert(parseScript("""a = load "file.csv" as (a, b, c); """) ==
      List(Load("a", "file.csv", Some(Schema(schema)))))
  }

  it should "should ignore comments" in {
    assert(parseScript("dump b; -- A comment") == List(Dump("b")))
  }

  it should "parse the dump statement" in {
    assert(parseScript("dump b;") == List(Dump("b")))
  }

  it should "parse the store statement" in {
    assert(parseScript("""store b into "file.csv";""") == List(Store("b", "file.csv")))
  }

  it should "parse a simple filter with a eq expression on named fields" in {
    assert(parseScript("a = filter b by x == y;") ==
      List(Filter("a", "b", Eq(RefExpr(NamedField("x")), RefExpr(NamedField("y"))))))
  }

  it should "parse a simple filter with a greater or equal expression on positional fields" in {
    assert(parseScript("a = FILTER b BY $1 >= $2;") ==
      List(Filter("a", "b", Geq(RefExpr(PositionalField(1)), RefExpr(PositionalField(2))))))
  }

  it should "parse a simple filter with a less than expression on fields and literals" in {
    assert(parseScript("a = filter b by $1 < 42;") ==
      List(Filter("a", "b", Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))))
  }

  it should "parse a simple foreach statement" in {
    assert(parseScript("a = foreach b generate x, y, z;") ==
      List(Foreach("a", "b", List(
        GeneratorExpr(RefExpr(NamedField("x"))),
        GeneratorExpr(RefExpr(NamedField("y"))),
        GeneratorExpr(RefExpr(NamedField("z")))
      ))))
  }

  it should "parse a foreach statement with aliases for fields" in {
    assert(parseScript("a = foreach b generate $0 as f1, $1 as f2, $2 as f3;") ==
      List(Foreach("a", "b", List(
        GeneratorExpr(RefExpr(PositionalField(0)), Some("f1")),
        GeneratorExpr(RefExpr(PositionalField(1)), Some("f2")),
        GeneratorExpr(RefExpr(PositionalField(2)), Some("f3"))
      ))))
  }

  it should "parse a foreach statement with field expressions" in {
    assert(parseScript("a = foreach b generate $0 + $1 as f1, $1 * 42 as f2;") ==
      List(Foreach("a", "b", List(
        GeneratorExpr(Add(RefExpr(PositionalField(0)), RefExpr(PositionalField(1))), Some("f1")),
        GeneratorExpr(Mult(RefExpr(PositionalField(1)), RefExpr(Value("42"))), Some("f2"))
      ))))
  }

  it should "parse a foreach statement with function expressions" in {
    assert(parseScript("""a = FOREACH b GENERATE TOMAP("field1", $0, "field2", $1);""") ==
      List(Foreach("a", "b", List(
        GeneratorExpr(Func("TOMAP", List(
          RefExpr(Value(""""field1"""")),
          RefExpr(PositionalField(0)),
          RefExpr(Value(""""field2"""")),
          RefExpr(PositionalField(1)))))
      ))))
  }

  it should "parse a foreach statement with another function expression" in {
    assert(parseScript("a = FOREACH b GENERATE f0, COUNT(f1) AS CNT;") ==
      List(Foreach("a", "b", List(
        GeneratorExpr(RefExpr(NamedField("f0"))),
        GeneratorExpr(Func("COUNT", List(RefExpr(NamedField("f1")))), Some("CNT"))
      ))))
  }

  it should "detect an invalid statement" in {
    intercept[java.lang.IllegalArgumentException] {
      parseScript("a = invalid b by x=y;")
    }
  }

  it should "parse a list of statements" in {
    assert(parseScript("a = load \"file.csv\";\ndump b;") == List(Load("a", "file.csv"), Dump("b")))
  }

  it should "parse a list of statements while ignoring comments" in {
    assert(parseScript("-- A comment\na = load \"file.csv\";-- Another comment\ndump b;") ==
      List(Load("a", "file.csv"), Dump("b")))
  }

  it should "parse a describe statement" in {
    assert(parseScript("describe x;") == List(Describe("x")))
  }

  it should "parse a limit statement" in {
    assert(parseScript("a = limit b 100;") == List(Limit("a", "b", 100)))
  }

  it should "parse a group by all statement" in {
    assert(parseScript("a = group b all;") == List(Grouping("a", "b", GroupingExpression(List()))))
  }

  it should "parse a group by statement with a single key" in {
    assert(parseScript("a = group b by $1;") == List(Grouping("a", "b", GroupingExpression(List(PositionalField(1))))))
  }

  it should "parse a group by statement with multiple keys" in {
    assert(parseScript("a = GROUP b BY ($0, $1);") ==
      List(Grouping("a", "b", GroupingExpression(List(PositionalField(0), PositionalField(1))))))
  }

  it should "parse a group by statement with multiple named keys" in {
    assert(parseScript("a = group b by (k1, k2, k3);") ==
      List(Grouping("a", "b", GroupingExpression(List(NamedField("k1"),
      NamedField("k2"), NamedField("k3"))))))
  }

  it should "parse the distinct statement" in {
    assert(parseScript("a = distinct b;") == List(Distinct("a", "b")))
  }

  it should "parse a binary join statement with simple expression" in {
    assert(parseScript("a = join b by $0, c by $0;") == List(Join("a", List("b", "c"),
      List(List(PositionalField(0)), List(PositionalField(0))))))
  }

  it should "parse a binary join statement with expression lists" in {
    assert(parseScript("a = join b by ($0, $1), c by ($1, $2);") == List(Join("a", List("b", "c"),
      List(List(PositionalField(0), PositionalField(1)), List(PositionalField(1), PositionalField(2))))))
  }

  it should "parse a multiway join statement " in {
    assert(parseScript("a = join b by $0, c by $0, d by $0;") == List(Join("a", List("b", "c", "d"),
      List(List(PositionalField(0)), List(PositionalField(0)), List(PositionalField(0))))))
  }
}
