package dbis.test.pig


/**
 * Created by kai on 01.04.15.
 */

import dbis.pig._
import org.scalatest.FlatSpec

class CompileSpec extends FlatSpec {
  def cleanString(s: String) : String = s.stripLineEnd.replaceAll("""\s+""", " ").trim

  "The compiler output" should "contain the Spark header & footer" in {
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitHeader("test") + codeGenerator.emitFooter)
    val expectedCode = cleanString("""
        |import org.apache.spark.SparkContext
        |import org.apache.spark.SparkContext._
        |import org.apache.spark.SparkConf
        |import org.apache.spark.rdd._
        |
        |object test {
        |    def main(args: Array[String]) {
        |      val conf = new SparkConf().setAppName("test_App")
        |      conf.setMaster("local[4]")
        |      val sc = new SparkContext(conf)
        |      sc.stop()
        |    }
        |}
      """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD" in {
    val op = Load("a", "file.csv")
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val a = sc.textFile("file.csv")""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for FILTER" in {
    val op = Filter("a", "b", Lt(PositionalField(1), Value("42")))
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.filter(t => {t(1) < 42})")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for DUMP" in {
    val op = Dump("a")
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("a.collect.map(t => println(t))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for STORE" in {
    val op = Store("a", "file.csv")
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""a.coalesce(1, true).saveAsTextFile("file.csv")""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for GROUP BY ALL" in {
    val op = Grouping("a", "b", GroupingExpression(List()))
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.glom")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for GROUP BY $0" in {
    val op = Grouping("a", "b", GroupingExpression(List(PositionalField(0))))
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.groupBy(t => {t(0)})")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for DISTINCT" in {
    val op = Distinct("a", "b")
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.distinct")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a binary join statement with simple expression" in {
    val op = Join("a", List("b", "c"), List(List(PositionalField(0)), List(PositionalField(0)))
    )
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val b_kv = b.keyBy(t => {t(0)})
      |val c_kv = c.keyBy(t => {t(0)})
      |val a = b_kv.join(c_kv)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a binary join statement with expression lists" in {
    val op = Join("a", List("b", "c"), List(List(PositionalField(0), PositionalField(1)),
      List(PositionalField(1), PositionalField(2)))
    )
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val b_kv = b.keyBy(t => {Array(t(0),t(1)).mkString})
      |val c_kv = c.keyBy(t => {Array(t(1),t(2)).mkString})
      |val a = b_kv.join(c_kv)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a multiway join statement" in {
    val op = Join("a", List("b", "c", "d"), List(List(PositionalField(0)),
      List(PositionalField(0)), List(PositionalField(0)))
    )
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val b_kv = b.keyBy(t => {t(0)})
      |val c_kv = c.keyBy(t => {t(0)})
      |val d_kv = d.keyBy(t => {t(0)})
      |val a = b_kv.join(c_kv).join(d_kv)""".stripMargin)
    assert(generatedCode == expectedCode)
  }
}
