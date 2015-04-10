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
    val op1 = Load("a", "file.csv")
    val op2 = Dump("a")
    val plan = new DataflowPlan(List(op1, op2))
    val compiler = new SparkCompile
    val result = compiler.compile("test", plan)
    // println(result)
  }

}
