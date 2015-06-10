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
import dbis.pig.PigCompiler._
import dbis.pig.op._
import dbis.pig.schema._
import org.scalatest.FlatSpec

class PigParserSpec extends FlatSpec {
  "The parser" should "parse a simple load statement" in  {
    assert(parseScript("""a = load 'file.csv';""") == List(Load("a", "file.csv")))
  }

  it should "parse also a case insensitive load statement" in  {
    assert(parseScript("""a = LOAD 'file.csv';""") == List(Load("a", "file.csv")))
  }

  it should "parse also a load statement with a path" in  {
    assert(parseScript("""a = LOAD 'dir1/dir2/file.csv';""") == List(Load("a", "dir1/dir2/file.csv")))
  }

  it should "parse also a load statement with the using clause" in  {
    assert(parseScript("""a = LOAD 'file.data' using PigStorage(',');""") ==
      List(Load("a", "file.data", None, "PigStorage", List("""','"""))))
    assert(parseScript("""a = LOAD 'file.n3' using RDFFileStorage();""") ==
      List(Load("a", "file.n3", None, "RDFFileStorage")))
  }

  it should "parse a load statement with typed schema specification" in {
    val schema = BagType("", TupleType("", Array(Field("a", Types.IntType),
                                                Field("b", Types.CharArrayType),
                                                Field("c", Types.DoubleType))))
    assert(parseScript("""a = load 'file.csv' as (a:int, b:chararray, c:double); """) ==
      List(Load("a", "file.csv", Some(Schema(schema)))))
  }

  it should "parse a load statement with complex typed schema specification" in {
    val schema = BagType("", TupleType("", Array(Field("a", Types.IntType),
      Field("t", TupleType("", Array(Field("f1", Types.IntType), Field("f2", Types.IntType)))),
      Field("b", BagType("", TupleType("t2", Array(Field("f3", Types.DoubleType), Field("f4", Types.DoubleType))))))))
    assert(parseScript("""a = load 'file.csv' as (a:int, t:tuple(f1: int, f2:int), b:{t2:tuple(f3:double, f4:double)}); """) ==
      List(Load("a", "file.csv", Some(Schema(schema)))))
  }

  it should "parse another load statement with complex typed schema specification" in {
    val schema = BagType("", TupleType("", Array(Field("a", Types.IntType),
      Field("m1", MapType("", Types.CharArrayType)),
      Field("m2", MapType("", TupleType("", Array(Field("f1", Types.IntType), Field("f2", Types.IntType))))),
      Field("m3", MapType("", Types.ByteArrayType)))))
    assert(parseScript("""a = load 'file.csv' as (a:int, m1:map[chararray], m2:[(f1: int, f2:int)], m3:[]); """) ==
      List(Load("a", "file.csv", Some(Schema(schema)))))
  }

  it should "parse a load statement with typed schema specification and using clause" in {
    val schema = BagType("", TupleType("", Array(Field("a", Types.IntType),
      Field("b", Types.CharArrayType),
      Field("c", Types.DoubleType))))
    assert(parseScript("""a = load 'file.data' using PigStorage() as (a:int, b:chararray, c:double); """) ==
      List(Load("a", "file.data", Some(Schema(schema)), "PigStorage")))
  }

  it should "parse a load statement with untyped schema specification" in {
    val schema = BagType("", TupleType("", Array(Field("a", Types.ByteArrayType),
      Field("b", Types.ByteArrayType),
      Field("c", Types.ByteArrayType))))
    assert(parseScript("""a = load 'file.csv' as (a, b, c); """) ==
      List(Load("a", "file.csv", Some(Schema(schema)))))
  }

  it should "should ignore comments" in {
    assert(parseScript("dump b; -- A comment") == List(Dump("b")))
  }

  it should "parse the dump statement" in {
    assert(parseScript("dump b;") == List(Dump("b")))
  }

  it should "parse the store statement" in {
    assert(parseScript("""store b into 'file.csv';""") == List(Store("b", "file.csv")))
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

  it should "parse a filter with a complex arithmetic expression" in {
    assert(parseScript("a = FILTER b BY x > (42 + y) * 3;") ==
      List(Filter("a", "b", Gt(RefExpr(NamedField("x")),
        Mult(Add(RefExpr(Value("42")), RefExpr(NamedField("y"))), RefExpr(Value("3")))))))
  }

  it should "parse a filter with a logical expression" in {
    assert(parseScript("a = FILTER b BY x > 0 AND y < 1;") ==
      List(Filter("a", "b", And(Gt(RefExpr(NamedField("x")), RefExpr(Value("0"))),
                                Lt(RefExpr(NamedField("y")), RefExpr(Value("1")))))))
  }

  it should "parse a filter with a complex logical expression" in {
    assert(parseScript("a = FILTER b BY x > 0 AND (y < 0 OR (NOT a == b));") ==
      List(Filter("a", "b", And(Gt(RefExpr(NamedField("x")), RefExpr(Value("0"))),
      Or(Lt(RefExpr(NamedField("y")), RefExpr(Value("0"))),
        Not(Eq(RefExpr(NamedField("a")), RefExpr(NamedField("b")))))))))
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
        GeneratorExpr(RefExpr(PositionalField(0)), Some(Field("f1"))),
        GeneratorExpr(RefExpr(PositionalField(1)), Some(Field("f2"))),
        GeneratorExpr(RefExpr(PositionalField(2)), Some(Field("f3")))
      ))))
  }

  it should "parse a foreach statement with field expressions" in {
    assert(parseScript("a = foreach b generate $0 + $1 as f1, $1 * 42 as f2;") ==
      List(Foreach("a", "b", List(
        GeneratorExpr(Add(RefExpr(PositionalField(0)), RefExpr(PositionalField(1))), Some(Field("f1"))),
        GeneratorExpr(Mult(RefExpr(PositionalField(1)), RefExpr(Value("42"))), Some(Field("f2")))
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
        GeneratorExpr(Func("COUNT", List(RefExpr(NamedField("f1")))), Some(Field("CNT", Types.ByteArrayType)))
      ))))
  }

  it should "parse a simple foreach statement with a schema" in {
    assert(parseScript("a = foreach b generate $0 as subj:chararray, $1 as pred, $2 as obj:chararray;") ==
      List(Foreach("a", "b", List(
        GeneratorExpr(RefExpr(PositionalField(0)), Some(Field("subj", Types.CharArrayType))),
        GeneratorExpr(RefExpr(PositionalField(1)), Some(Field("pred", Types.ByteArrayType))),
        GeneratorExpr(RefExpr(PositionalField(2)), Some(Field("obj", Types.CharArrayType)))
      ))))
  }

  it should "detect an invalid statement" in {
    intercept[java.lang.IllegalArgumentException] {
      parseScript("a = invalid b by x=y;")
    }
  }

  it should "parse a list of statements" in {
    assert(parseScript("a = load 'file.csv';\ndump b;") == List(Load("a", "file.csv"), Dump("b")))
  }

  it should "parse a list of statements while ignoring comments" in {
    assert(parseScript("-- A comment\na = load 'file.csv';-- Another comment\ndump b;") ==
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

  it should "parse a multiway join statement" in {
    assert(parseScript("a = join b by $0, c by $0, d by $0;") == List(Join("a", List("b", "c", "d"),
      List(List(PositionalField(0)), List(PositionalField(0)), List(PositionalField(0))))))
  }

  it should "parse expressions with deref operators for map" in {
    assert(parseScript("""a = foreach b generate m1#"k1", m1#"k2";""") ==
      List(Foreach("a", "b", List(GeneratorExpr(RefExpr(DerefMap(NamedField("m1"), """"k1""""))),
        GeneratorExpr(RefExpr(DerefMap(NamedField("m1"), """"k2"""")))))))
  }

  it should "parse expressions with deref operators on positional fields for map" in {
    assert(parseScript("""a = foreach b generate $0#"k1", $1#"k2";""") ==
      List(Foreach("a", "b", List(GeneratorExpr(RefExpr(DerefMap(PositionalField(0), """"k1""""))),
        GeneratorExpr(RefExpr(DerefMap(PositionalField(1), """"k2"""")))))))
  }

  it should "parse expressions with deref operators for tuple and bag" in {
    assert(parseScript("""a = foreach b generate t1.k, t2.$0;""") ==
      List(Foreach("a", "b", List(GeneratorExpr(RefExpr(DerefTuple(NamedField("t1"), NamedField("k")))),
        GeneratorExpr(RefExpr(DerefTuple(NamedField("t2"), PositionalField(0))))))))
  }

  it should "parse expressions with deref operators on positional fields for tuple and bag" in {
    assert(parseScript("""a = foreach b generate $0.$1, $2.$0;""") ==
      List(Foreach("a", "b", List(GeneratorExpr(RefExpr(DerefTuple(PositionalField(0), PositionalField(1)))),
        GeneratorExpr(RefExpr(DerefTuple(PositionalField(2), PositionalField(0))))))))
  }

  it should "parse a binary union statement" in {
    assert(parseScript("a = union b, c;") == List(Union("a", List("b", "c"))))
  }

  it should "parse a n-ary union statement" in {
    assert(parseScript("a = union b, c, d, e;") == List(Union("a", List("b", "c", "d", "e"))))
  }

  it should "parse a register statement" in {
    assert(parseScript("""register "/usr/local/share/myfile.jar";""") == List(Register(""""/usr/local/share/myfile.jar"""")))
  }

  it should "parse a stream statement without schema" in {
    assert(parseScript("a = stream b through package.myOp;") == List(StreamOp("a", "b", "package.myOp")))
  }

  it should "parse a stream statement with parameters" in {
    assert(parseScript("a = stream b through myOp(1.0, 42);") == List(StreamOp("a", "b", "myOp", Some(List(Value("1.0"), Value("42"))))))
  }

  it should "parse a stream statement with schema" in {
    val schema = BagType("", TupleType("", Array(Field("f1", Types.IntType),
      Field("f2", Types.DoubleType))))
    assert(parseScript("a = stream b through myOp as (f1: int, f2:double);") == List(StreamOp("a", "b", "myOp", None, Some(Schema(schema)))))
  }

  it should "parse a sample statement with a given size" in {
    assert(parseScript("a = sample b 0.10;") == List(Sample("a", "b", RefExpr(Value("0.10")))))
  }

  it should "parse a sample statement with an expression" in {
    assert(parseScript("a = sample b 100/num_rows;") ==
      List(Sample("a", "b", Div(RefExpr(Value("100")), RefExpr(NamedField("num_rows"))))))
  }

  it should "parse a simple order by statement" in {
    assert(parseScript("a = order b by $0;") ==
      List(OrderBy("a", "b", List(OrderBySpec(PositionalField(0), OrderByDirection.AscendingOrder)))))
  }

  it should "parse a simple order by statement on whole tuples" in {
    assert(parseScript("a = order b by * desc;") ==
      List(OrderBy("a", "b", List(OrderBySpec(Value("*"), OrderByDirection.DescendingOrder)))))
  }

  it should "parse a simple order by statement with ascending sort order" in {
    assert(parseScript("a = order b by f1 asc;") ==
      List(OrderBy("a", "b", List(OrderBySpec(NamedField("f1"), OrderByDirection.AscendingOrder)))))
  }

  it should "parse a simple order by statement with descending sort order" in {
    assert(parseScript("a = order b by $1 desc;") ==
      List(OrderBy("a", "b", List(OrderBySpec(PositionalField(1), OrderByDirection.DescendingOrder)))))
  }

  it should "parse an order by statement with multiple fields" in {
    assert(parseScript("a = order b by $1 desc, $2 asc;") ==
      List(OrderBy("a", "b", List(OrderBySpec(PositionalField(1), OrderByDirection.DescendingOrder),
        OrderBySpec(PositionalField(2), OrderByDirection.AscendingOrder)))))
  }
}
