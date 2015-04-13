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
    assert(parseScript("a = filter b by x == y;") == List(Filter("a", "b", Eq(NamedField("x"), NamedField("y")))))
  }

  it should "parse a simple filter with a greater or equal expression on positional fields" in {
    assert(parseScript("a = filter b by $1 >= $2;") == List(Filter("a", "b", Geq(PositionalField(1), PositionalField(2)))))
  }

  it should "parse a simple filter with a less than expression on fields and literals" in {
    assert(parseScript("a = filter b by $1 < 42;") == List(Filter("a", "b", Lt(PositionalField(1), Value("42")))))
  }

  it should "parse a simple foreach statement" in {
    assert(parseScript("a = foreach b generate x, y, z;") == List(Foreach("a", "b", List("x", "y", "z"))))
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

  it should "parse a group by all statement" in {
    assert(parseScript("a = group b all;") == List(Grouping("a", "b", GroupingExpression(List()))))
  }

  it should "parse a group by statement with a single key" in {
    assert(parseScript("a = group b by $1;") == List(Grouping("a", "b", GroupingExpression(List(PositionalField(1))))))
  }

  it should "parse a group by statement with multiple keys" in {
    assert(parseScript("a = group b by ($0, $1);") ==
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
