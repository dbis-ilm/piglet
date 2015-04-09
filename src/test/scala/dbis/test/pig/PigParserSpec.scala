/**
 * Created by kai on 31.03.15.
 */
package dbis.test.pig

import dbis.pig._
import dbis.pig.PigCompiler._
import org.scalatest.FlatSpec

class PigParserSpec extends FlatSpec {
  "The parser" should "parse a simple load statement" in  {
    assert(parseScript("a = load \"file.csv\";") == List(Load("a", "file.csv")))
  }

  it should "should ignore comments" in {
    assert(parseScript("dump b; -- A comment") == List(Dump("b")))
  }

  it should "parse the dump statement" in {
    assert(parseScript("dump b;") == List(Dump("b")))
  }

  it should "parse a simple filter with a eq expression" in {
    assert(parseScript("a = filter b by x=y;") == List(Filter("a", "b", Eq("x", "y"))))
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
}
