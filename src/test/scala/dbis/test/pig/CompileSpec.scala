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

  it should "contain produce for LOAD" in {
    val op = Load("a", "file.csv")
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val a = sc.textFile("file.csv")""")
    assert(generatedCode == expectedCode)
  }

  it should "contain produce for FILTER" in {
    val op = Filter("a", "b", Lt(PositionalField(1), Value("42")))
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.filter(t => {t(1) < 42})")
    assert(generatedCode == expectedCode)
  }

  it should "contain produce for DUMP" in {
    val op = Dump("a")
    val codeGenerator = new SparkGenCode
    val generatedCode = codeGenerator.emitNode(op)
    val expectedCode = cleanString("a.collect.map(t => println(t))")
    assert(generatedCode == expectedCode)
  }

  it should "contain produce for GROUP BY ALL" in {
    val op = Grouping("a", "b", GroupingExpression(List()))
    val codeGenerator = new SparkGenCode
    val generatedCode = codeGenerator.emitNode(op)
    val expectedCode = cleanString("val a = b.glom")
    assert(generatedCode == expectedCode)
  }

  it should "contain produce for GROUP BY $0" in {
    val op = Grouping("a", "b", GroupingExpression(List(PositionalField(0))))
    val codeGenerator = new SparkGenCode
    val generatedCode = codeGenerator.emitNode(op)
    val expectedCode = cleanString("val a = b.groupBy(t => {t(0)})")
    assert(generatedCode == expectedCode)
  }
}
