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

import java.net.URI

import dbis.pig.plan.DataflowPlan
import dbis.test.TestTools._

import dbis.pig.expr._
import dbis.pig.op._
import dbis.pig.op.cmd._
import dbis.pig.parser.LanguageFeature
import dbis.pig.parser.PigParser.parseScript
import dbis.pig.schema._
import org.scalatest.{Matchers, OptionValues, FlatSpec}

import scala.util.Random

class PigParserSpec extends FlatSpec with OptionValues with Matchers {
  /* -------------------- LOAD -------------------- */
  "The parser" should "parse a simple load statement" in  {
    val uri = new URI("file.csv")
    assert(parseScript("""a = load 'file.csv';""") == List(Load(Pipe("a"), uri)))
  }

  it should "parse also a case insensitive load statement" in  {
    val uri = new URI("file.csv")
    assert(parseScript("""a = LOAD 'file.csv';""") == List(Load(Pipe("a"), uri)))
  }

  it should "parse also a load statement with a path" in  {
    val uri = new URI("dir1/dir2/file.csv")
    assert(parseScript("""a = LOAD 'dir1/dir2/file.csv';""") == List(Load(Pipe("a"), uri)))
  }

  it should "parse also a load statement with the using clause" in  {
    val uri1 = new URI("file.data")
    val uri2 = new URI("file.n3")
    assert(parseScript("""a = LOAD 'file.data' using PigStorage(',');""") ==
      List(Load(Pipe("a"), uri1, None, Some("PigStorage"), List("""",""""))))
    assert(parseScript("""a = LOAD 'file.n3' using RDFFileStorage();""") ==
      List(Load(Pipe("a"), uri2, None, Some("RDFFileStorage"))))
  }

  it should "parse a load statement with typed schema specification" in {
    val uri = new URI("file.csv")
    val schema = Array(Field("a", Types.IntType),
                                                Field("b", Types.CharArrayType),
                                                Field("c", Types.DoubleType))
    assert(parseScript("""a = load 'file.csv' as (a:int, b:chararray, c:double); """) ==
      List(Load(Pipe("a"), uri, Some(Schema(schema)))))
  }

  it should "parse a load statement with complex typed schema specification" in {
    val uri = new URI("file.csv")
    val schema = Array(Field("a", Types.IntType),
      Field("t", TupleType(Array(Field("f1", Types.IntType), Field("f2", Types.IntType)))),
      Field("b", BagType(TupleType(Array(Field("f3", Types.DoubleType), Field("f4", Types.DoubleType))))))
    assert(parseScript("""a = load 'file.csv' as (a:int, t:tuple(f1: int, f2:int), b:{tuple(f3:double, f4:double)}); """) ==
      List(Load(Pipe("a"), uri, Some(Schema(schema)))))
  }

  it should "parse another load statement with complex typed schema specification" in {
    val uri = new URI("file.csv")
    val schema = BagType(TupleType(Array(Field("a", Types.IntType),
      Field("m1", MapType(Types.CharArrayType)),
      Field("m2", MapType(TupleType(Array(Field("f1", Types.IntType), Field("f2", Types.IntType))))),
      Field("m3", MapType(Types.ByteArrayType)))))
    assert(parseScript("""a = load 'file.csv' as (a:int, m1:map[chararray], m2:[(f1: int, f2:int)], m3:[]); """) ==
      List(Load(Pipe("a"), uri, Some(Schema(schema)))))
  }

  it should "parse a load statement with typed schema specification and using clause" in {
    val uri = new URI("file.data")
    val schema = BagType(TupleType(Array(Field("a", Types.IntType),
      Field("b", Types.CharArrayType),
      Field("c", Types.DoubleType))))
    assert(parseScript("""a = load 'file.data' using PigStorage() as (a:int, b:chararray, c:double); """) ==
      List(Load(Pipe("a"), uri, Some(Schema(schema)), Some("PigStorage"))))
  }

  it should "parse a load statement with untyped schema specification" in {
    val uri = new URI("file.csv")
    val schema = BagType(TupleType(Array(Field("a", Types.ByteArrayType),
      Field("b", Types.ByteArrayType),
      Field("c", Types.ByteArrayType))))
    assert(parseScript("""a = load 'file.csv' as (a, b, c); """) ==
      List(Load(Pipe("a"), uri, Some(Schema(schema)))))
  }

  /* -------------------- Comments -------------------- */
  it should "ignore comments" in {
    assert(parseScript("dump b; -- A comment") == List(Dump(Pipe("b"))))
  }

  it should "handle comments only to EOL" in {
    assert(parseScript("""
      |-- A comment
      |-- Another comment
      |dump b; -- A comment
      |dump c;""".stripMargin) == List(Dump(Pipe("b")), Dump(Pipe("c"))))
  }

  /* -------------------- DUMP -------------------- */
  it should "parse the dump statement" in {
    assert(parseScript("dump b;") == List(Dump(Pipe("b"))))
  }

  /* -------------------- STORE -------------------- */
  it should "parse the store statement" in {
    val uri = new URI("file.csv")
    assert(parseScript("""store b into 'file.csv';""") == List(Store(Pipe("b"), uri)))
  }

  /* -------------------- FILTER -------------------- */
  it should "parse a simple filter with a eq expression on named fields" in {
    assert(parseScript("a = filter b by x == y;") ==
      List(Filter(Pipe("a"), Pipe("b"), Eq(RefExpr(NamedField("x")), RefExpr(NamedField("y"))))))
  }

  it should "parse a simple filter with a greater or equal expression on positional fields" in {
    assert(parseScript("a = FILTER b BY $1 >= $2;") ==
      List(Filter(Pipe("a"), Pipe("b"), Geq(RefExpr(PositionalField(1)), RefExpr(PositionalField(2))))))
  }

  it should "parse a simple filter with a less than expression on fields and literals" in {
    assert(parseScript("a = filter b by $1 < 42;") ==
      List(Filter(Pipe("a"), Pipe("b"), Lt(RefExpr(PositionalField(1)), RefExpr(Value(42))))))
  }

  it should "parse a filter with a complex arithmetic expression" in {
    assert(parseScript("a = FILTER b BY x > (42 + y) * 3;") ==
      List(Filter(Pipe("a"), Pipe("b"), Gt(RefExpr(NamedField("x")),
        Mult(PExpr(Add(RefExpr(Value(42)), RefExpr(NamedField("y")))), RefExpr(Value(3)))))))
  }

  it should "parse a filter with a logical expression" in {
    assert(parseScript("a = FILTER b BY x > 0 AND y < 1;") ==
      List(Filter(Pipe("a"), Pipe("b"), And(Gt(RefExpr(NamedField("x")), RefExpr(Value(0))),
                                Lt(RefExpr(NamedField("y")), RefExpr(Value(1)))))))
  }

  it should "parse a filter with a complex logical expression" in {
    assert(parseScript("a = FILTER b BY x > 0 AND (y < 0 OR (NOT a == b));") ==
      List(Filter(Pipe("a"), Pipe("b"), And(Gt(RefExpr(NamedField("x")), RefExpr(Value(0))),
      PPredicate(Or(Lt(RefExpr(NamedField("y")), RefExpr(Value(0))),
        PPredicate(Not(Eq(RefExpr(NamedField("a")), RefExpr(NamedField("b")))))))))))
  }

  it should "parse a filter with another complex logical expression" in {
    assert(parseScript("a = FILTER b BY x > 0 AND y < 0 OR NOT a == b;") ==
      List(Filter(Pipe("a"), Pipe("b"), Or(
        And(Gt(RefExpr(NamedField("x")), RefExpr(Value(0))),
          Lt(RefExpr(NamedField("y")), RefExpr(Value(0)))),
          Not(Eq(RefExpr(NamedField("a")), RefExpr(NamedField("b"))))
      ))))
  }

  it should "parse a filter with yet another complex logical expression" in {
    assert(parseScript("a = FILTER b BY x > 0 OR y < 0 OR a == b;") ==
      List(Filter(Pipe("a"), Pipe("b"), Or(Or(
          Gt(RefExpr(NamedField("x")), RefExpr(Value(0))),
          Lt(RefExpr(NamedField("y")), RefExpr(Value(0)))),
          Eq(RefExpr(NamedField("a")), RefExpr(NamedField("b")))
      ))))
  }

  it should "parse a filter with a function expression" in {
    assert(parseScript("a = FILTER b BY STARTSWITH($0,\"test\") == true;") ==
      List(Filter(Pipe("a"), Pipe("b"), Eq(
                                          Func("STARTSWITH", List(RefExpr(PositionalField(0)), RefExpr(Value("\"test\"")))),
                                          RefExpr(Value(true))))))
  }

  it should "parse a filter with a function expression and number" in {
    assert(parseScript("a = FILTER b BY aFunc(x, y) > 0;") ==
      List(Filter(Pipe("a"), Pipe("b"), Gt(
        Func("aFunc", List(RefExpr(NamedField("x")), RefExpr(NamedField("y")))),
        RefExpr(Value(0))))))
  }

  it should "parse a filter with an expression on a string literal" in {
    assert(parseScript("a = FILTER b BY x == 'aString';") ==
      List(Filter(Pipe("a"), Pipe("b"), Eq(
        RefExpr(NamedField("x")),
        RefExpr(Value("aString"))))))
  }

  it should "parse a filter with a boolean function expression" in {
    assert(parseScript("a = FILTER b BY STARTSWITH($0,\"test\");") ==
      List(Filter(Pipe("a"), Pipe("b"),
        Eq(Func("STARTSWITH", List(RefExpr(PositionalField(0)), RefExpr(Value("\"test\"")))), RefExpr(Value(true))))))
  }
  
  it should "parse a filter with a function expression and boolean" in {
    assert(parseScript("a = FILTER b BY aFunc(x, y) == true AND cFunc(x, y) >= x;") ==
      List(Filter(Pipe("a"),Pipe("b"),And(
            Eq(Func("aFunc",List(RefExpr(NamedField("x")), RefExpr(NamedField("y")))),RefExpr(Value(true))),
            Geq(Func("cFunc",List(RefExpr(NamedField("x")), RefExpr(NamedField("y")))),RefExpr(NamedField("x")))),false))) 
  }

  /* -------------------- FOREACH -------------------- */
  it should "parse a simple foreach statement" in {
    assert(parseScript("a = foreach b generate x, y, z;") ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(
        GeneratorExpr(RefExpr(NamedField("x"))),
        GeneratorExpr(RefExpr(NamedField("y"))),
        GeneratorExpr(RefExpr(NamedField("z")))
      )))))
  }

  it should "parse a simple foreach statement with *" in {
    assert(parseScript("a = foreach b generate *;") ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(
        GeneratorExpr(RefExpr(NamedField("*")))
      )))))
  }

  it should "parse a foreach statement with aliases for fields" in {
    assert(parseScript("a = foreach b generate $0 as f1, $1 as f2, $2 as f3;") ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(
        GeneratorExpr(RefExpr(PositionalField(0)), Some(Field("f1"))),
        GeneratorExpr(RefExpr(PositionalField(1)), Some(Field("f2"))),
        GeneratorExpr(RefExpr(PositionalField(2)), Some(Field("f3")))
      )))))
  }

  it should "parse a foreach statement with field expressions" in {
    assert(parseScript("a = foreach b generate $0 + $1 as f1, $1 * 42 as f2;") ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(
        GeneratorExpr(Add(RefExpr(PositionalField(0)), RefExpr(PositionalField(1))), Some(Field("f1"))),
        GeneratorExpr(Mult(RefExpr(PositionalField(1)), RefExpr(Value(42))), Some(Field("f2")))
      )))))
  }

  it should "parse a foreach statement with flatten" in {
    assert(parseScript("a = foreach b generate $0, flatten($1);") ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(
        GeneratorExpr(RefExpr(PositionalField(0))),
        GeneratorExpr(FlattenExpr(RefExpr(PositionalField(1))))
      )))))
  }

  it should "parse a foreach statement with function expressions" in {
    assert(parseScript("""a = FOREACH b GENERATE TOMAP("field1", $0, "field2", $1);""") ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(
        GeneratorExpr(Func("TOMAP", List(
          RefExpr(Value("\"field1\"")),
          RefExpr(PositionalField(0)),
          RefExpr(Value("\"field2\"")),
          RefExpr(PositionalField(1)))))
      )))))
  }

  it should "parse a foreach statement with another function expression" in {
    assert(parseScript("a = FOREACH b GENERATE f0, COUNT(f1) AS CNT;") ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(
        GeneratorExpr(RefExpr(NamedField("f0"))),
        GeneratorExpr(Func("COUNT", List(RefExpr(NamedField("f1")))), Some(Field("CNT", Types.ByteArrayType)))
      )))))
  }

  it should "parse a simple foreach statement with a schema" in {
    assert(parseScript("a = foreach b generate $0 as subj:chararray, $1 as pred, $2 as obj:chararray;") ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(
        GeneratorExpr(RefExpr(PositionalField(0)), Some(Field("subj", Types.CharArrayType))),
        GeneratorExpr(RefExpr(PositionalField(1)), Some(Field("pred", Types.ByteArrayType))),
        GeneratorExpr(RefExpr(PositionalField(2)), Some(Field("obj", Types.CharArrayType)))
      )))))
  }

  it should "parse a FOREACH statement with a bag constructor" in {
    assert(parseScript("a = FOREACH b GENERATE {$0, $1, $2} as myBag;") ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(
        GeneratorExpr(ConstructBagExpr(List(RefExpr(PositionalField(0)),
                                            RefExpr(PositionalField(1)),
                                            RefExpr(PositionalField(2)))), Some(Field("myBag", Types.ByteArrayType))
        ))))))
  }

  it should "parse a FOREACH statement with a tuple constructor" in {
    assert(parseScript("a = FOREACH b GENERATE $0, ($1, $2) as myTuple;") ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(
      GeneratorExpr(RefExpr(PositionalField(0))),
      GeneratorExpr(ConstructTupleExpr(List(RefExpr(PositionalField(1)),
                                            RefExpr(PositionalField(2)))), Some(Field("myTuple", Types.ByteArrayType))
      ))))))
  }

  it should "parse a FOREACH statement with a map constructor" in {
    assert(parseScript("a = FOREACH b GENERATE [$0, $1] as myMap;") ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(
        GeneratorExpr(ConstructMapExpr(List(RefExpr(PositionalField(0)),
                                            RefExpr(PositionalField(1)))), Some(Field("myMap", Types.ByteArrayType))
      ))))))
  }

  /* -------------------- nested FOREACH -------------------- */
  it should "parse a simple nested FOREACH statement" in {
    assert(parseScript(
      """a = FOREACH b {
      |generate c, COUNT(d);
      |};""".stripMargin) ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorPlan(List(
        Generate(List(GeneratorExpr(RefExpr(NamedField("c"))),
          GeneratorExpr(Func("COUNT", List(RefExpr(NamedField("d")))))))
      )))))
  }

  it should "parse a nested FOREACH statement with multiple statements" in {
    assert(parseScript(
      """a = FOREACH b {
        |data = d.dat;
        |unique_d = DISTINCT data;
        |generate key, COUNT(unique_d);
        |};""".stripMargin) ==
    List(Foreach(Pipe("a"), Pipe("b"), GeneratorPlan(List(
    ConstructBag(Pipe("data"), DerefTuple(NamedField("d"), NamedField("dat"))),
    Distinct(Pipe("unique_d"), Pipe("data")),
    Generate(List(GeneratorExpr(RefExpr(NamedField("key"))),
      GeneratorExpr(Func("COUNT", List(RefExpr(NamedField("unique_d")))))))
    )))))
  }

  /* -------------------- ACCUMULATE -------------------- */
  it should "parse a simple accumulate statement" in {
    assert(parseScript("a = ACCUMULATE b GENERATE COUNT($0), AVG($1), SUM($2);") ==
      List(Accumulate(Pipe("a"), Pipe("b"), GeneratorList(List(
        GeneratorExpr(Func("COUNT", List(RefExpr(PositionalField(0))))),
        GeneratorExpr(Func("AVG", List(RefExpr(PositionalField(1))))),
        GeneratorExpr(Func("SUM", List(RefExpr(PositionalField(2)))))
      )))))
  }

  /* -------------------- invalid statements -------------------- */
  it should "detect an invalid statement" in {
    intercept[java.lang.IllegalArgumentException] {
      parseScript("a = invalid b by x=y;")
    }
  }

  it should "parse a list of statements" in {
    val uri = new URI("file.csv")
    assert(parseScript("a = load 'file.csv';\ndump b;") == List(Load(Pipe("a"), uri), Dump(Pipe("b"))))
  }

  it should "parse a list of statements while ignoring comments" in {
    val uri = new URI("file.csv")
    assert(parseScript("-- A comment\na = load 'file.csv';-- Another comment\ndump b;") ==
      List(Load(Pipe("a"), uri), Dump(Pipe("b"))))
  }

  it should "parse a describe statement" in {
    assert(parseScript("describe x;") == List(Describe(Pipe("x"))))
  }

  it should "parse a limit statement" in {
    assert(parseScript("a = limit b 100;") == List(Limit(Pipe("a"), Pipe("b"), 100)))
  }

  it should "parse a group by all statement" in {
    assert(parseScript("a = group b all;") == List(Grouping(Pipe("a"), Pipe("b"), GroupingExpression(List()))))
  }

  it should "parse a group by statement with a single key" in {
    assert(parseScript("a = group b by $1;") == List(Grouping(Pipe("a"), Pipe("b"), GroupingExpression(List(PositionalField(1))))))
  }

  it should "parse a group by statement with multiple keys" in {
    assert(parseScript("a = GROUP b BY ($0, $1);") ==
      List(Grouping(Pipe("a"), Pipe("b"), GroupingExpression(List(PositionalField(0), PositionalField(1))))))
  }

  it should "parse a group by statement with multiple named keys" in {
    assert(parseScript("a = group b by (k1, k2, k3);") ==
      List(Grouping(Pipe("a"), Pipe("b"), GroupingExpression(List(NamedField("k1"),
      NamedField("k2"), NamedField("k3"))))))
  }

  it should "parse the distinct statement" in {
    assert(parseScript("a = distinct b;") == List(Distinct(Pipe("a"), Pipe("b"))))
  }

  it should "parse a binary join statement with simple expression" in {
    assert(parseScript("a = join b by $0, c by $0;") == List(Join(Pipe("a"), List(Pipe("b"), Pipe("c")),
      List(List(PositionalField(0)), List(PositionalField(0))))))
  }

  it should "parse a binary join statement with expression lists" in {
    assert(parseScript("a = join b by ($0, $1), c by ($1, $2);") == List(Join(Pipe("a"), List(Pipe("b"), Pipe("c")),
      List(List(PositionalField(0), PositionalField(1)), List(PositionalField(1), PositionalField(2))))))
  }

  it should "parse a multiway join statement" in {
    assert(parseScript("a = join b by $0, c by $0, d by $0;") == List(Join(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")),
      List(List(PositionalField(0)), List(PositionalField(0)), List(PositionalField(0))))))
  }

  it should "parse a binary cross statement" in {
    assert(parseScript("a = cross b, c;") == List(Cross(Pipe("a"), List(Pipe("b"), Pipe("c")))))
  }

  it should "parse a n-ary cross statement" in {
    assert(parseScript("a = cross b, c, d, e;") == List(Cross(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d"), Pipe("e")))))
  }


  it should "parse expressions with deref operators for map" in {
    assert(parseScript("""a = foreach b generate m1#"k1", m1#"k2";""") ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(GeneratorExpr(RefExpr(DerefMap(NamedField("m1"), "\"k1\""))),
        GeneratorExpr(RefExpr(DerefMap(NamedField("m1"), "\"k2\""))))))))
  }

  it should "parse expressions with deref operators on positional fields for map" in {
    assert(parseScript("""a = foreach b generate $0#"k1", $1#"k2";""") ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(GeneratorExpr(RefExpr(DerefMap(PositionalField(0), "\"k1\""))),
        GeneratorExpr(RefExpr(DerefMap(PositionalField(1), "\"k2\""))))))))
  }

  it should "parse expressions with deref operators for tuple and bag" in {
    assert(parseScript("""a = foreach b generate t1.k, t2.$0;""") ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(GeneratorExpr(RefExpr(DerefTuple(NamedField("t1"), NamedField("k")))),
        GeneratorExpr(RefExpr(DerefTuple(NamedField("t2"), PositionalField(0)))))))))
  }

  it should "parse expressions with deref operators on positional fields for tuple and bag" in {
    assert(parseScript("""a = foreach b generate $0.$1, $2.$0;""") ==
      List(Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(GeneratorExpr(RefExpr(DerefTuple(PositionalField(0), PositionalField(1)))),
        GeneratorExpr(RefExpr(DerefTuple(PositionalField(2), PositionalField(0)))))))))
  }

  it should "parse a binary union statement" in {
    assert(parseScript("a = union b, c;") == List(Union(Pipe("a"), List(Pipe("b"), Pipe("c")))))
  }

  it should "parse a n-ary union statement" in {
    assert(parseScript("a = union b, c, d, e;") == List(Union(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d"), Pipe("e")))))
  }

  it should "parse a register statement" in {
    assert(parseScript("""register '/usr/local/share/myfile.jar';""") == List(RegisterCmd("/usr/local/share/myfile.jar")))
  }

  it should "parse a define (function alias) statement" in {
    assert(parseScript("""define myFunc class.func();""") == List(DefineCmd("myFunc", "class.func", List())))
  }

  it should "parse a define (function alias) statement with constructor parameters" in {
    assert(parseScript("""define myFunc class.func(42, "Hallo");""") ==
      List(DefineCmd("myFunc", "class.func", List(Value(42), Value("\"Hallo\"")))))
  }

  it should "parse a SET statement" in {
    assert(parseScript("set parallelism 5;") == List(SetCmd("parallelism", Value(5))))
    assert(parseScript("""set baseDirectory "/home/user";""") == List(SetCmd("baseDirectory", Value("\"/home/user\""))))
  }

  it should "parse a stream statement without schema" in {
    assert(parseScript("a = stream b through package.myOp;") == List(StreamOp(Pipe("a"), Pipe("b"), "package.myOp")))
  }

  it should "parse a stream statement with parameters" in {
    assert(parseScript("a = stream b through myOp(1.0, 42);") == List(StreamOp(Pipe("a"), Pipe("b"), "myOp", Some(List(Value(1.0), Value(42))))))
  }

  it should "parse a stream statement with schema" in {
    val schema = BagType(TupleType(Array(Field("f1", Types.IntType),
      Field("f2", Types.DoubleType))))
    assert(parseScript("a = stream b through myOp as (f1: int, f2:double);") == List(StreamOp(Pipe("a"), Pipe("b"), "myOp", None, Some(Schema(schema)))))
  }

  it should "parse a sample statement with a given size" in {
    assert(parseScript("a = sample b 0.10;") == List(Sample(Pipe("a"), Pipe("b"), RefExpr(Value(0.10)))))
  }

  it should "parse a sample statement with an expression" in {
    assert(parseScript("a = sample b 100/num_rows;") ==
      List(Sample(Pipe("a"), Pipe("b"), Div(RefExpr(Value(100)), RefExpr(NamedField("num_rows"))))))
  }

  it should "parse a simple order by statement" in {
    assert(parseScript("a = order b by $0;") ==
      List(OrderBy(Pipe("a"), Pipe("b"), List(OrderBySpec(PositionalField(0), OrderByDirection.AscendingOrder)))))
  }

  it should "parse a simple order by statement on whole tuples" in {
    assert(parseScript("a = order b by * desc;") ==
      List(OrderBy(Pipe("a"), Pipe("b"), List(OrderBySpec(Value("*"), OrderByDirection.DescendingOrder)))))
  }

  it should "parse a simple order by statement with ascending sort order" in {
    assert(parseScript("a = order b by f1 asc;") ==
      List(OrderBy(Pipe("a"), Pipe("b"), List(OrderBySpec(NamedField("f1"), OrderByDirection.AscendingOrder)))))
  }

  it should "parse a simple order by statement with descending sort order" in {
    assert(parseScript("a = order b by $1 desc;") ==
      List(OrderBy(Pipe("a"), Pipe("b"), List(OrderBySpec(PositionalField(1), OrderByDirection.DescendingOrder)))))
  }

  it should "parse an order by statement with multiple fields" in {
    assert(parseScript("a = order b by $1 desc, $2 asc;") ==
      List(OrderBy(Pipe("a"), Pipe("b"), List(OrderBySpec(PositionalField(1), OrderByDirection.DescendingOrder),
        OrderBySpec(PositionalField(2), OrderByDirection.AscendingOrder)))))
  }

  it should "parse a socket read statement in standard mode" in {
    assert(parseScript("a = socket_read '127.0.0.1:5555';", LanguageFeature.StreamingPig)
      == List(SocketRead(Pipe("a"), SocketAddress("","127.0.0.1","5555"), "")))
  }

  it should "parse a socket read statement in standard mode with using clause" in {
    assert(parseScript("a = socket_read '127.0.0.1:5555' using PigStream(',');", LanguageFeature.StreamingPig)
      == List(SocketRead(Pipe("a"), SocketAddress("","127.0.0.1","5555"), "", None, Some("PigStream"),List("""",""""))))
    assert(parseScript("a = socket_read '127.0.0.1:5555' using RDFStream();", LanguageFeature.StreamingPig)
      == List(SocketRead(Pipe("a"), SocketAddress("","127.0.0.1","5555"), "", None, Some("RDFStream"))))
  }

  it should "parse a socket read statement in zmq mode" in {
    assert(parseScript("a = socket_read 'tcp://127.0.0.1:5555' mode zmq;", LanguageFeature.StreamingPig)
      == List(SocketRead(Pipe("a"), SocketAddress("tcp://","127.0.0.1","5555"), "zmq")))
  }

  it should "parse a socket write statement in standard mode" in {
    assert(parseScript("socket_write a to '127.0.0.1:5555';", LanguageFeature.StreamingPig)
      == List(SocketWrite(Pipe("a"), SocketAddress("","127.0.0.1","5555"), "")))
  }

  it should "parse a socket write statement in zmq mode" in {
    assert(parseScript("socket_write a to 'tcp://127.0.0.1:5555' mode zmq;", LanguageFeature.StreamingPig)
      == List(SocketWrite(Pipe("a"), SocketAddress("tcp://","127.0.0.1","5555"), "zmq")))
  }

  it should "parse a window statement using Rows for window and slider" in {
    assert(parseScript("a = window b rows 100 slide rows 10;", LanguageFeature.StreamingPig)
      == List(Window(Pipe("a"), Pipe("b"), (100,""), (10,""))))
  }
 
  it should "parse a window statement using Range for window and slider" in {
    assert(parseScript("a = window b range 100 SECONDS slide range 10 SECONDS;", LanguageFeature.StreamingPig)
      == List(Window(Pipe("a"), Pipe("b"), (100,"SECONDS"), (10,"SECONDS"))))
  }

  it should "parse a SPLIT INTO statement" in {
    assert(parseScript("SPLIT a INTO b IF $0 > 12, c IF $0 < 12, d IF $0 == 0;") ==
      List(SplitInto(Pipe("a"), List(
        SplitBranch(Pipe("b"), Gt(RefExpr(PositionalField(0)), RefExpr(Value(12)))),
        SplitBranch(Pipe("c"), Lt(RefExpr(PositionalField(0)), RefExpr(Value(12)))),
        SplitBranch(Pipe("d"), Eq(RefExpr(PositionalField(0)), RefExpr(Value(0))))
    ))))
  }

  it should "reject statements not supported in the plain language feature" in {
    intercept[java.lang.IllegalArgumentException] {
      parseScript("a = TUPLIFY b ON $0;")
    }
  }

  it should "accept TUPLIFY in SparqlPig" in {
    assert(parseScript("a = TUPLIFY b ON $0;", LanguageFeature.SparqlPig) ==
      List(Tuplify(Pipe("a"), Pipe("b"), PositionalField(0))))
  }

  it should "parse BGP_FILTER in SparqlPig" in {
    assert(parseScript( """a = BGP_FILTER b BY { $0 "firstName" "Stefan" };""", LanguageFeature.SparqlPig) ==
      List(BGPFilter(Pipe("a"), Pipe("b"), List(TriplePattern(PositionalField(0), Value("\"firstName\""), Value
        ("\"Stefan\""))
      ))))
  }

  it should "parse BGP_FILTER with variables in SparqlPig" in {
    assert(parseScript( """a = BGP_FILTER b BY { ?a "firstName" "Stefan" };""", LanguageFeature.SparqlPig) ==
      List(BGPFilter(Pipe("a"), Pipe("b"), List(TriplePattern(NamedField("a"), Value("\"firstName\""), Value
        ("\"Stefan\""))
      ))))
  }

  it should "parse BGP_FILTER with a complex pattern" in {
    assert(parseScript( """a = BGP_FILTER b BY { $0 "firstName" "Stefan" . $0 "lastName" "Hage" };""",
      LanguageFeature.SparqlPig) ==
      List(BGPFilter(Pipe("a"), Pipe("b"), List(TriplePattern(PositionalField(0), Value("\"firstName\""), Value("\"Stefan\"")),
        TriplePattern(PositionalField(0), Value("\"lastName\""), Value("\"Hage\""))))))
  }

  it should "parse RDFLoad operators for plain triples" in {
    val uri = new URI("rdftest.rdf")
    val ungrouped = parseScript( """a = RDFLoad('rdftest.rdf');""", LanguageFeature.SparqlPig)
    assert(ungrouped == List(RDFLoad(Pipe("a"), uri, None)))
    
    val expected = Some(Schema(BagType(TupleType(Array(
      Field("subject", Types.CharArrayType),
      Field("predicate", Types.CharArrayType),
      Field("object", Types.CharArrayType))))))

    /* the classname is set by a global variable, which might have
     * different values for different test case orderings. 
     * However, we don't care about the classname here and simply set it
     * manually to ensure equality. 
     */
    expected.get.className = ungrouped.head.schema.get.className
    
    ungrouped.head.schema shouldBe expected
  }

  it should "parse RDFLoad operators for triple groups" in {
    val uri = new URI("rdftest.rdf")
    val grouped_on_subj = parseScript( """a = RDFLoad('rdftest.rdf') grouped on subject;""", LanguageFeature.SparqlPig)
    val grouped_on_pred = parseScript( """a = RDFLoad('rdftest.rdf') grouped on predicate;""", LanguageFeature.SparqlPig)
    val grouped_on_obj = parseScript( """a = RDFLoad('rdftest.rdf') grouped on object;""", LanguageFeature.SparqlPig)
    assert(grouped_on_subj == List(RDFLoad(Pipe("a"), uri, Some("subject"))))
    assert(grouped_on_pred == List(RDFLoad(Pipe("a"), uri, Some("predicate"))))
    assert(grouped_on_obj == List(RDFLoad(Pipe("a"), uri, Some("object"))))
    assert(grouped_on_subj.head.schema.get == RDFLoad.groupedSchemas("subject"))
    assert(grouped_on_pred.head.schema.get == RDFLoad.groupedSchemas("predicate"))
    assert(grouped_on_obj.head.schema.get == RDFLoad.groupedSchemas("object"))
  }

  it should "reject RDFLoad operators with unknown grouping column names" in {
    val colname = Random.nextString(10)
    intercept[IllegalArgumentException] {
      val grouped_on_subj = parseScript( """a = RDFLoad('rdftest.rdf') grouped on $colname;""", LanguageFeature
        .SparqlPig)
    }
  }

  it should "parse a matcher statement using only mode" in {
    assert(parseScript("a = MATCH_EVENT b PATTERN seq (A, B) WITH (A: x == 0) MODE skip_till_next_match;", LanguageFeature.ComplexEventPig)
      == List(Matcher(Pipe("a"), Pipe("b"),
        SeqPattern(List(SimplePattern("A"), SimplePattern("B"))),
        CompEvent(List(SimpleEvent(SimplePattern("A"), Eq(RefExpr(NamedField("x")), RefExpr(Value(0)))))),
        "skip_till_next_match",
        (0, "SECONDS"))))
  }

  it should "parse a matcher statement using skip_till_any_match mode" in {
    assert(parseScript("a = MATCH_EVENT b PATTERN seq (A, B) WITH (A: x == 0) MODE skip_till_any_match;", LanguageFeature.ComplexEventPig)
      == List(Matcher(Pipe("a"), Pipe("b"),
        SeqPattern(List(SimplePattern("A"), SimplePattern("B"))),
        CompEvent(List(SimpleEvent(SimplePattern("A"), Eq(RefExpr(NamedField("x")), RefExpr(Value(0)))))),
        "skip_till_any_match",
        (0, "SECONDS"))))
  }

  it should "parse a matcher statement using only window" in {
    assert(parseScript("a = MATCH_EVENT b PATTERN seq (A, B) WITH (A: x == 0) WITHIN 30 SECONDS;", LanguageFeature.ComplexEventPig)
      == List(Matcher(Pipe("a"), Pipe("b"),
        SeqPattern(List(SimplePattern("A"), SimplePattern("B"))),
        CompEvent(List(SimpleEvent(SimplePattern("A"), Eq(RefExpr(NamedField("x")), RefExpr(Value(0)))))),
        "skip_till_next_match",
        (30, "SECONDS"))))
  }

  it should "parse a matcher statement using a simple event" in {
    assert(parseScript("a = MATCH_EVENT b PATTERN A WITH (A: x == 0) WITHIN 30 SECONDS;", LanguageFeature.ComplexEventPig)
      == List(Matcher(Pipe("a"), Pipe("b"),
        SimplePattern("A"),
        CompEvent(List(SimpleEvent(SimplePattern("A"), Eq(RefExpr(NamedField("x")), RefExpr(Value(0)))))),
        "skip_till_next_match",
        (30, "SECONDS"))))
  }
  it should "parse a matcher statement using a sequence and three events" in {
    assert(parseScript("a = MATCH_EVENT b PATTERN seq (A, B, C) WITH (A: x == 0) MODE skip_till_next_match WITHIN 30 SECONDS;", LanguageFeature.ComplexEventPig)
      == List(Matcher(Pipe("a"), Pipe("b"),
        SeqPattern(List(SimplePattern("A"), SimplePattern("B"), SimplePattern("C"))),
        CompEvent(List(SimpleEvent(SimplePattern("A"), Eq(RefExpr(NamedField("x")), RefExpr(Value(0)))))),
        "skip_till_next_match",
        (30, "SECONDS"))))
  }
  it should "parse a matcher statement using a sequence event with negation" in {
    assert(parseScript("a = MATCH_EVENT b PATTERN seq (A, neg(B), C) WITH (A: x == 0) MODE skip_till_next_match WITHIN 30 SECONDS;", LanguageFeature.ComplexEventPig)
      == List(Matcher(Pipe("a"), Pipe("b"),
        SeqPattern(List(SimplePattern("A"), NegPattern(SimplePattern("B")), SimplePattern("C"))),
        CompEvent(List(SimpleEvent(SimplePattern("A"), Eq(RefExpr(NamedField("x")), RefExpr(Value(0)))))),
        "skip_till_next_match",
        (30, "SECONDS"))))
  }

  it should "parse a matcher statement using simple event definitions" in {
    assert(parseScript("a = MATCH_EVENT b PATTERN seq (A, neg(B), C) WITH (A: x == 0, C: x == 1) MODE skip_till_next_match WITHIN 30 SECONDS;", LanguageFeature.ComplexEventPig)
      == List(Matcher(Pipe("a"), Pipe("b"),
        SeqPattern(List(SimplePattern("A"), NegPattern(SimplePattern("B")), SimplePattern("C"))),
        CompEvent(List(SimpleEvent(SimplePattern("A"), Eq(RefExpr(NamedField("x")), RefExpr(Value(0)))),
          SimpleEvent(SimplePattern("C"), Eq(RefExpr(NamedField("x")), RefExpr(Value(1)))))),
        "skip_till_next_match",
        (30, "SECONDS"))))
  }
  it should "parse a matcher statement using composite sequence pattern" in {
    assert(parseScript("a = MATCH_EVENT b PATTERN seq (A, seq(B, D), C) WITH (A: x == 0, C: x == 1, D: y == (x / 10)) MODE skip_till_next_match WITHIN 30 SECONDS;", LanguageFeature.ComplexEventPig)
      == List(Matcher(Pipe("a"), Pipe("b"),
        SeqPattern(List(SimplePattern("A"),
          SeqPattern(List(SimplePattern("B"), SimplePattern("D"))), SimplePattern("C"))),
        CompEvent(List(SimpleEvent(SimplePattern("A"), Eq(RefExpr(NamedField("x")), RefExpr(Value(0)))),
          SimpleEvent(SimplePattern("C"), Eq(RefExpr(NamedField("x")), RefExpr(Value(1)))),
          SimpleEvent(SimplePattern("D"), Eq(RefExpr(NamedField("y")), PExpr(Div(RefExpr(NamedField("x")), RefExpr(Value(10)))))))),
        "skip_till_next_match",
        (30, "SECONDS"))))
  }

  it should "parse a matcher statement using conjunction and disjunction" in {
    assert(parseScript("a = MATCH_EVENT b PATTERN AND (A, OR(B, D), C) WITH (A: x == 0, C: x == 1, D: y == (x / 10)) MODE skip_till_next_match WITHIN 30 SECONDS;", LanguageFeature.ComplexEventPig)
      == List(Matcher(Pipe("a"), Pipe("b"),
        ConjPattern(List(SimplePattern("A"),
          DisjPattern(List(SimplePattern("B"), SimplePattern("D"))), SimplePattern("C"))),
        CompEvent(List(SimpleEvent(SimplePattern("A"), Eq(RefExpr(NamedField("x")), RefExpr(Value(0)))),
          SimpleEvent(SimplePattern("C"), Eq(RefExpr(NamedField("x")), RefExpr(Value(1)))),
          SimpleEvent(SimplePattern("D"), Eq(RefExpr(NamedField("y")), PExpr(Div(RefExpr(NamedField("x")), RefExpr(Value(10)))))))),
        "skip_till_next_match",
        (30, "SECONDS"))))
  }

  it should "parse HDFS commands" in {
    assert(parseScript("fs -copyToRemote /usr/local/file /hdfs/data/file;")
      == List(HdfsCmd("copyToRemote", List("/usr/local/file", "/hdfs/data/file"))))
    assert(parseScript("fs -copyFromLocal /hdfs/data/file /usr/local/file;")
      == List(HdfsCmd("copyFromLocal", List("/hdfs/data/file", "/usr/local/file"))))
    assert(parseScript("fs -rmdir /hdfs/data;")
      == List(HdfsCmd("rmdir", List("/hdfs/data"))))

    intercept[java.lang.IllegalArgumentException] {
      parseScript("fs -unknownCmd something;")
    }
  }

  it should "parse a script with embedded code" in {
    val ops = parseScript(
      """
        |<% def someFunc(s: String): String = {
        | s
        |}
        |%>
        |A = LOAD 'file.csv';
      """.stripMargin)
      assert (ops(1) == Load(Pipe("A"), new URI("file.csv")))
  }

  it should "parse a script with embedded code and rules" in {
    val ops = parseScript(
      """
        |<% def someFunc(s: String): String = {
        |   s
        | }
        |rules:
        |def rule(term: Any): Option[PigOperator] = None
        |def rule2(term: Any): Option[PigOperator] = None
        |%>
        |A = LOAD 'file.csv';
      """.stripMargin)
    assert(ops(0).isInstanceOf[EmbedCmd])
    val op = ops(0).asInstanceOf[EmbedCmd]
    assert(op.code.stripLineEnd ==
       """def someFunc(s: String): String = {
          |   s
          | }""".stripMargin)
    assert(op.ruleCode.headOption.value ==
      """
        |def rule(term: Any): Option[PigOperator] = None
        |def rule2(term: Any): Option[PigOperator] = None
        |""".stripMargin)
  }


  it should "parse lineage information for NamedFields" in {
    val ops = parseScript(
      """
         B = ORDER A BY A::B::foo;
         C = ORDER B BY B::foo;
      """.
        stripMargin)
    val nf = ops.headOption.value.asInstanceOf[OrderBy].orderSpec.headOption.value.field.asInstanceOf[NamedField]
    nf.name shouldBe "foo"
    nf.lineage shouldBe List("A", "B")
    val nf2 = ops.lastOption.value.asInstanceOf[OrderBy].orderSpec.headOption.value.field.asInstanceOf[NamedField]
    nf2.name shouldBe "foo"
    nf2.lineage shouldBe List("B")
  }

  it should "parse a DEFINE macro statement" in {
    val ops = parseScript(
    """DEFINE my_macro(in_alias, p) RETURNS out_alias {
      |$out_alias = FILTER $in_alias BY $0 == $p;
      |};
    """.stripMargin
    )
    assert(ops == List(DefineMacroCmd(Pipe("out_alias"), "my_macro", Some(List("in_alias", "p")),
                        List(Filter(Pipe("$out_alias"), Pipe("$in_alias"),
                          Eq(RefExpr(PositionalField(0)), RefExpr(NamedField("$p")))
                        ))
    )))
  }

  it should "parse a DEFINE macro statement without parameters" in {
    val ops = parseScript(
      """DEFINE my_macro() RETURNS out_alias {
        |$out_alias = LOAD 'file' USING PigStorage(':');
        |};
      """.stripMargin
    )
    assert(ops == List(DefineMacroCmd(Pipe("out_alias"), "my_macro", Some(List()),
      List(Load(Pipe("$out_alias"), new URI("file"), None, Some("PigStorage"), List("""":"""")))
    )))
  }

  it should "parse a statement invoking a macro" in {
    assert(parseScript("a = my_macro(in, 42);") == List(MacroOp(Pipe("a"), "my_macro", Some(List(NamedField("in"), Value(42))))))
  }


  it should "parse a load statement with schema and timestamp specification" in {
    val uri = new URI("file.csv")
    val schema = Schema(Array(Field("a", Types.IntType),
      Field("b", Types.CharArrayType),
      Field("c", Types.DoubleType)))
    schema.timestampField = 0
    assert(parseScript("""a = load 'file.csv' as (a:int, b:chararray, c:double) timestamp(a);""") ==
      List(Load(Pipe("a"), uri, Some(schema))))
  }

  it should "parse a load statement with schema and positional timestamp specification" in {
    val uri = new URI("file.csv")
    val schema = Schema(Array(Field("a", Types.IntType),
      Field("b", Types.CharArrayType),
      Field("c", Types.LongType)))
    schema.timestampField = 2
    assert(parseScript("""a = load 'file.csv' as (a:int, b:chararray, c:long) timestamp($2);""") ==
      List(Load(Pipe("a"), uri, Some(schema))))
  }

  it should "parse a socket_read statement with schema and timestamp" in {
    val schema = Schema(Array(Field("f1", Types.LongType),
      Field("f2", Types.CharArrayType)))
    schema.timestampField = 0
    assert(parseScript("a = SOCKET_READ 'localhost:5555' AS (f1: long, f2: chararray) TIMESTAMP(f1);", LanguageFeature.StreamingPig)
      == List(SocketRead(Pipe("a"), SocketAddress("","localhost","5555"), "", Some(schema))))
  }

  it should "parse a LOAD statement with matrix types" in {
    val uri = new URI("file.csv")
    val schema = Schema(Array(Field("m1", MatrixType(Types.IntType, 4, 4, MatrixRep.DenseMatrix))))
    assert(parseScript("""a = load 'file.csv' as (m1: dimatrix(4,4));""") ==
      List(Load(Pipe("a"), uri, Some(schema))))
  }

  it should "parse a FOREACH statement with a matrix constructor" in {
    assert(parseScript("o = FOREACH in GENERATE simatrix(3, 5, $1) as myMat;") ==
      List(Foreach(Pipe("o"), Pipe("in"), GeneratorList(List(
          GeneratorExpr(ConstructMatrixExpr("si",
            3, 5, RefExpr(PositionalField(1))),
            Some(Field("myMat", Types.ByteArrayType)))
      )))))
  }
}
