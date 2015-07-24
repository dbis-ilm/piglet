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
package dbis.test.flink

import dbis.pig.PigCompiler._
import dbis.pig.codegen.ScalaBackendGenCode
import dbis.pig.op._
import dbis.pig.plan.DataflowPlan
import dbis.pig.schema._
import org.scalatest.FlatSpec
import dbis.pig.backends.BackendManager

class FlinkCompileSpec extends FlatSpec {
  def cleanString(s: String) : String = s.stripLineEnd.replaceAll("""\s+""", " ").trim
  val templateFile = BackendManager.backend("flink").templateFile

  "The compiler output" should "contain the Flink header & footer" in {
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitImport 
      + codeGenerator.emitHeader1("test") 
      + codeGenerator.emitHeader2("test") 
      + codeGenerator.emitFooter)
    val expectedCode = cleanString("""
      |import org.apache.flink.streaming.api.scala._
      |import dbis.flink._
      |import java.util.concurrent.TimeUnit
      |
      |object test {
      |    def main(args: Array[String]) {
      |        val env = StreamExecutionEnvironment.getExecutionEnvironment
      |        env.execute("Starting Query")
      |    }
      |}
    """.stripMargin)
  assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD" in {
    val op = Load(Pipe("a"), "file.csv")
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val file = new java.io.File(".").getCanonicalPath + "/file.csv"
    val expectedCode = cleanString(s"""val a = PigStorage().load(env, "${file}", '\\t')""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD with PigStorage" in {
    val op = Load(Pipe("a"), "file.csv", None, "PigStorage", List("""','"""))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val file = new java.io.File(".").getCanonicalPath + "/file.csv"
    val expectedCode = cleanString(s"""val a = PigStorage().load(env, "${file}", ',')""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD with RDFFileStorage" in {
    val op = Load(Pipe("a"), "file.n3", None, "RDFFileStorage")
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val file = new java.io.File(".").getCanonicalPath + "/file.n3"
    val expectedCode = cleanString(s"""val a = RDFFileStorage().load(env, "${file}")""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for FILTER" in { 
    val op = Filter(Pipe("a"), Pipe("b"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))) 
    val codeGenerator = new ScalaBackendGenCode(templateFile) 
    val generatedCode = cleanString(codeGenerator.emitNode(op)) 
    val expectedCode = cleanString("val a = b.filter(t => {t(1) < 42})") 
    assert(generatedCode == expectedCode) 
  }

  it should "contain code for DUMP" in {
    val op = Dump(Pipe("a"))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""a.map(_.mkString(",")).print""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for STORE" in {
    val op = Store(Pipe("A"), "file.csv")
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val file = new java.io.File(".").getCanonicalPath + "/file.csv"
    val expectedCode = cleanString(s"""A.map(_.mkString(",")).writeAsText("${file}")""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for DISTINCT" in {
    val op = Distinct(Pipe("a"), Pipe("b"))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LIMIT" in {
    val op = Limit(Pipe("a"), Pipe("b"), 10)
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.window(Count.of(10)).every(Time.of(5, TimeUnit.SECONDS))")
    assert(generatedCode == expectedCode)
  }


  it should "contain code for a FOREACH statement with function expressions" in {
    // a = FOREACH b GENERATE TOMAP("field1", $0, "field2", $1);
    val op = Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(
      GeneratorExpr(Func("TOMAP", List(
        RefExpr(Value("\"field1\"")),
        RefExpr(PositionalField(0)),
        RefExpr(Value("\"field2\"")),
        RefExpr(PositionalField(1)))))
      )))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.map(t => List(PigFuncs.toMap(\"field1\",t(0),\"field2\",t(1))))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a FOREACH statement with another function expression" in {
    // a = FOREACH b GENERATE $0, COUNT($1) AS CNT;
    val op = Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(
      GeneratorExpr(RefExpr(PositionalField(0))),
      GeneratorExpr(Func("COUNT", List(RefExpr(PositionalField(1)))), Some(Field("CNT", Types.LongType)))
      )))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.map(t => List(t(0),PigFuncs.count(t(1).asInstanceOf[Seq[Any]])))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for deref operator on maps in FOREACH statement" in {
    // a = FOREACH b GENERATE $0#"k1", $1#"k2";
    val op = Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(GeneratorExpr(RefExpr(DerefMap(PositionalField(0), "\"k1\""))),
      GeneratorExpr(RefExpr(DerefMap(PositionalField(1), "\"k2\""))))))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.map(t => List(t(0).asInstanceOf[Map[String,Any]]("k1"),t(1).asInstanceOf[Map[String,Any]]("k2")))""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for deref operator on tuple in FOREACH statement" in {
    // a = FOREACH b GENERATE $0.$1, $2.$0;
    val op = Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(GeneratorExpr(RefExpr(DerefTuple(PositionalField(0), PositionalField(1)))),
      GeneratorExpr(RefExpr(DerefTuple(PositionalField(2), PositionalField(0)))))))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val a = b.map(t => List(t(0).asInstanceOf[List[Any]](1),t(2).asInstanceOf[List[Any]](0)))""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a UNION operator on two relations" in {
    // a = UNION b, c;
    val op = Union(Pipe("a"), List(Pipe("b"), Pipe("c")))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.union(c)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a UNION operator on more than two relations" in {
    // a = UNION b, c, d;
    val op = Union(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.union(c).union(d)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a binary JOIN statement with simple expression" in {
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c")), List(List(PositionalField(0)), List(PositionalField(0))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
                                                              Field("f2", Types.DoubleType),
                                                              Field("f3", Types.IntType)))))
    val input1 = Pipe("b",Load(Pipe("b"), "file.csv", Some(schema), "PigStorage", List("\",\"")))
    val input2 = Pipe("c",Load(Pipe("c"), "file.csv", Some(schema), "PigStorage", List("\",\"")))
    op.inputs=List(input1,input2)
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.join(c).onWindow(5, TimeUnit.SECONDS).where(t => t(0)).equalTo(t => t(0)).map{
        |t => t._1 ++ t._2
        |}""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a binary JOIN statement with expression lists" in {
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c")), List(List(PositionalField(0), PositionalField(1)),
      List(PositionalField(1), PositionalField(2))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
                                                              Field("f2", Types.DoubleType),
                                                              Field("f3", Types.IntType)))))
    val input1 = Pipe("b",Load(Pipe("b"), "file.csv", Some(schema), "PigStorage", List("\",\"")))
    val input2 = Pipe("c",Load(Pipe("c"), "file.csv", Some(schema), "PigStorage", List("\",\"")))
    op.inputs=List(input1,input2)
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.join(c).onWindow(5, TimeUnit.SECONDS).where(t => Array(t(0),t(1)).mkString).equalTo(t => Array(t(1),t(2)).mkString).map{
        |t => t._1 ++ t._2
        |}""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a multiway JOIN statement" in {
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")), List(List(PositionalField(0)),
      List(PositionalField(0)), List(PositionalField(0))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
                                                              Field("f2", Types.DoubleType),
                                                              Field("f3", Types.IntType)))))
    val input1 = Pipe("b",Load(Pipe("b"), "file.csv", Some(schema), "PigStorage", List("\",\"")))
    val input2 = Pipe("c",Load(Pipe("c"), "file.csv", Some(schema), "PigStorage", List("\",\"")))
    val input3 = Pipe("d",Load(Pipe("d"), "file.csv", Some(schema), "PigStorage", List("\",\"")))
    op.inputs=List(input1,input2,input3)
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val a = b.join(c).onWindow(5, TimeUnit.SECONDS).where(t => t(0)).equalTo(t => t(0)).map{ 
        |t => t._1 ++ t._2
        |}.join(d).onWindow(5, TimeUnit.SECONDS).where(t => t(0)).equalTo(t => t(0)).map{
        |t => t._1 ++ t._2
        |}""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for GROUP BY ALL" in {
    val op = Grouping(Pipe("a"), Pipe("b"), GroupingExpression(List()))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b"
      /*"""
        |val fields = new ListBuffer[Int]
        |for(i <- 0 to b.getType.getTotalFields()-1)(fields+=i)
        |val a = b.groupBy(fields.toList:_*)
        |""".stripMargin*/
    )
    assert(generatedCode == expectedCode)
  }

  it should "contain code for GROUP BY $0" in {
    val op = Grouping(Pipe("a"), Pipe("b"), GroupingExpression(List(PositionalField(0))))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.groupBy(t => t(0))")
    assert(generatedCode == expectedCode)
  }


  it should "contain code for SOCKET_READ" in {
    val op = SocketRead(Pipe("a"), SocketAddress("", "localhost", "9999"), "")
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val a = PigStream().connect(env, "localhost", 9999, '\t')""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for SOCKET_READ with PigStream" in {
    val op = SocketRead(Pipe("a"), SocketAddress("", "localhost", "9999"), "", None, "PigStream", List("""','"""))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(s"""val a = PigStream().connect(env, "localhost", 9999, ',')""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for SOCKET_READ with RDFStream" in {
    val op = SocketRead(Pipe("a"), SocketAddress("", "localhost", "9999"), "", None, "RDFStream")
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val a = RDFStream().connect(env, "localhost", 9999)""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for SOCKET_READ in ZMQ mode" in {
    val op = SocketRead(Pipe("a"), SocketAddress("tcp://", "localhost", "9999"), "zmq")
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val a = PigStream().zmqSubscribe(env, "tcp://localhost:9999", '\t')""")
    assert(generatedCode == expectedCode)
  }

}
