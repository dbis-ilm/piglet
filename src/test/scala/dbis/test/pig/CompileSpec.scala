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
        |import dbis.spark._
        |
        |object test {
        |    def main(args: Array[String]) {
        |      val conf = new SparkConf().setAppName("test_App")
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

  it should "contain code for LOAD with PigStorage" in {
    val op = Load("a", "file.csv", None, "PigStorage", List("""",""""))
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    println(generatedCode)
    val expectedCode = cleanString("""val a = PigStorage().load(sc, "file.csv", ",")""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD with RDFFileStorage" in {
    val op = Load("a", "file.n3", None, "RDFFileStorage")
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val a = RDFFileStorage().load(sc, "file.n3")""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for FILTER" in {
    val op = Filter("a", "b", Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.filter(t => {t(1) < 42})")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for DUMP" in {
    val op = Dump("a")
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""a.collect.map(t => println(t.mkString(",")))""")
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
    val expectedCode = cleanString("val a = b.groupBy(t => {t(0)}).map{case (k,v) => List(k,v)}")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for DISTINCT" in {
    val op = Distinct("a", "b")
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.distinct")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for Limit" in {
    val op = Limit("a", "b", 10)
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = sc.parallelize(b.take(10))")
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
      |val a = b_kv.join(c_kv).map{case (k,v) => List(k,v)}""".stripMargin)
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
      |val a = b_kv.join(c_kv).map{case (k,v) => List(k,v)}""".stripMargin)
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
      |val a = b_kv.join(c_kv).join(d_kv).map{case (k,v) => List(k,v)}""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code a foreach statement with function expressions" in {
    // a = FOREACH b GENERATE TOMAP("field1", $0, "field2", $1);
    val op = Foreach("a", "b", List(
      GeneratorExpr(Func("TOMAP", List(
        RefExpr(Value(""""field1"""")),
        RefExpr(PositionalField(0)),
        RefExpr(Value(""""field2"""")),
        RefExpr(PositionalField(1)))))
    ))
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    println(generatedCode)
    val expectedCode = cleanString("val a = b.map(t => List(PigFuncs.toMap(\"field1\",t(0),\"field2\",t(1))))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a foreach statement with another function expression" in {
    // a = FOREACH b GENERATE $0, COUNT($1) AS CNT;
    val op = Foreach("a", "b", List(
        GeneratorExpr(RefExpr(PositionalField(0))),
        GeneratorExpr(Func("COUNT", List(RefExpr(PositionalField(1)))), Some("CNT"))
      ))
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.map(t => List(t(0),PigFuncs.count(t(1).asInstanceOf[Seq[Any]])))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for deref operator in foreach statement" in {
    val op = Foreach("a", "b", List(GeneratorExpr(RefExpr(DerefMap(PositionalField(0), """"k1""""))),
      GeneratorExpr(RefExpr(DerefMap(PositionalField(1), """"k2"""")))))
    val codeGenerator = new SparkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val a = b.map(t => List(t(0).asInstanceOf[Map[String,Any]]("k1"),t(1).asInstanceOf[Map[String,Any]]("k2")))""".stripMargin)
        assert(generatedCode == expectedCode)
  }
}
