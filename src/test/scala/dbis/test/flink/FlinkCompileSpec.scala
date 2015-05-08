package dbis.test.flink


/**
  * Created by Philipp on 08.05.15.
  */

import dbis.pig._
import org.scalatest.FlatSpec

class FlinkCompileSpec extends FlatSpec {
  def cleanString(s: String) : String = s.stripLineEnd.replaceAll("""\s+""", " ").trim

  "The compiler output" should "contain the Flink header & footer" in {
    val codeGenerator = new FlinkGenCode
    val generatedCode = cleanString(codeGenerator.emitHeader("test") + codeGenerator.emitFooter)
    val expectedCode = cleanString("""
      |import org.apache.flink.streaming.api.scala._
      |import java.util.concurrent.TimeUnit
      |
      |object test {
      |    def main(args: Array[String]) {
      |        val env = StreamExecutionEnvironment.getExecutionEvironment
      |        env.execute("Starting Flink Query")
      |    }
      |}
    """.stripMargin)
  assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD" in {
    val op = Load("a", "file.csv")
    val codeGenerator = new FlinkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val a = env.readTextFile("file.csv")""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD with PigStorage" in {
    val op = Load("a", "file.csv", None, "PigStorage", List("""",""""))
    val codeGenerator = new FlinkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val a = PigStorage().load(env, "file.csv", ",")""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD with RDFFileStorage" in {
    val op = Load("a", "file.n3", None, "RDFFileStorage")
    val codeGenerator = new FlinkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val a = RDFFileStorage().load(env, "file.n3")""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for FILTER" in { 
    val op = Filter("a", "b", Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))) 
    val codeGenerator = new FlinkGenCode 
    val generatedCode = cleanString(codeGenerator.emitNode(op)) 
    val expectedCode = cleanString("val a = b.filter(t => {t(1) < 42})") 
    assert(generatedCode == expectedCode) 
  }

  it should "contain code for DUMP" in {
    val op = Dump("a")
    val codeGenerator = new FlinkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""a.print()""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for STORE" in {
    val op = Store("a", "file.csv")
    val codeGenerator = new FlinkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""a.writeAsText("file.csv")""")
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
    val codeGenerator = new FlinkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.map(t => List(PigFuncs.toMap(\"field1\",t(0),\"field2\",t(1))))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a foreach statement with another function expression" in {
    // a = FOREACH b GENERATE $0, COUNT($1) AS CNT;
    val op = Foreach("a", "b", List(
      GeneratorExpr(RefExpr(PositionalField(0))),
      GeneratorExpr(Func("COUNT", List(RefExpr(PositionalField(1)))), Some(Field("CNT", Types.LongType)))
      ))
    val codeGenerator = new FlinkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.map(t => List(t(0),PigFuncs.count(t(1).asInstanceOf[Seq[Any]])))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for deref operator on maps in foreach statement" in {
    // a = FOREACH b GENERATE $0#"k1", $1#"k2";
    val op = Foreach("a", "b", List(GeneratorExpr(RefExpr(DerefMap(PositionalField(0), """"k1""""))),
      GeneratorExpr(RefExpr(DerefMap(PositionalField(1), """"k2"""")))))
    val codeGenerator = new FlinkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.map(t => List(t(0).asInstanceOf[Map[String,Any]]("k1"),t(1).asInstanceOf[Map[String,Any]]("k2")))""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for deref operator on tuple in foreach statement" in {
    // a = FOREACH b GENERATE $0.$1, $2.$0;
    val op = Foreach("a", "b", List(GeneratorExpr(RefExpr(DerefTuple(PositionalField(0), PositionalField(1)))),
      GeneratorExpr(RefExpr(DerefTuple(PositionalField(2), PositionalField(0))))))
    val codeGenerator = new FlinkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val a = b.map(t => List(t(0).asInstanceOf[List[Any]](1),t(2).asInstanceOf[List[Any]](0)))""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a union operator on two relations" in {
    // a = UNION b, c;
    val op = Union("a", List("b", "c"))
    val codeGenerator = new FlinkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.merge(c)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a union operator on more than two relations" in {
    // a = UNION b, c, d;
    val op = Union("a", List("b", "c", "d"))
    val codeGenerator = new FlinkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.merge(c,d)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a binary join statement with simple expression" in {
    val op = Join("a", List("b", "c"), List(List(PositionalField(0)), List(PositionalField(0)))
    )
    val codeGenerator = new FlinkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val b_k = b.map(t => {t(0)})
        |val c_k = c.map(t => {t(0)})
        |val a = b.join(c).onWindow(5, TimeUnit.SECONDS).where(b_k).equalTo(c_k)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a binary join statement with expression lists" in {
    val op = Join("a", List("b", "c"), List(List(PositionalField(0), PositionalField(1)),
      List(PositionalField(1), PositionalField(2)))
    )
    val codeGenerator = new FlinkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val b_k = b.map(t => {Array(t(0),t(1)).mkString})
        |val c_k = c.map(t => {Array(t(1),t(2)).mkString})
        |val a = b.join(c).onWindow(5, TimeUnit.SECONDS).where(b_k).equalTo(c_k)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a multiway join statement" in {
    val op = Join("a", List("b", "c", "d"), List(List(PositionalField(0)),
      List(PositionalField(0)), List(PositionalField(0)))
    )
    val codeGenerator = new FlinkGenCode
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val b_k = b.map(t => {t(0)})
        |val c_k = c.map(t => {t(0)})
        |val d_k = d.map(t => {t(0)})
        |val a = b.join(c).onWindow(5, TimeUnit.SECONDS).where(b_k).equalTo(c_k).join(d).onWindow(5, TimeUnit.SECONDS).where(b_k).equalTo(d_k)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

}
