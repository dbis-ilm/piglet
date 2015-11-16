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
package dbis.test.spark

import dbis.pig.backends.BackendManager
import dbis.pig.codegen.BatchCodeGen
import dbis.pig.codegen.StreamingCodeGen
import dbis.pig.expr._
import dbis.pig.parser.LanguageFeature
import dbis.pig.parser.PigParser.parseScript
import dbis.pig.backends.BackendManager
import dbis.pig.codegen.{StreamingCodeGen, BatchCodeGen}
import dbis.pig.op._
import dbis.pig.plan.DataflowPlan
import dbis.pig.plan.rewriting.Rewriter._
import dbis.pig.plan.rewriting.Rules
import dbis.pig.plan.rewriting.Rules
import dbis.pig.schema._
import dbis.pig.udf.UDFTable
import dbis.test.TestTools._
import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}

class StreamingCompileSpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  override def beforeAll() {
    Rules.registerAllRules()
  }

  def cleanString(s: String): String = s.stripLineEnd.replaceAll( """\s+""", " ").trim

  val backendConf = BackendManager.backend("sparks")
  BackendManager.backend = backendConf
  val templateFile = backendConf.templateFile

  "The compiler output" should "contain the Spark Streaming header & footer" in {
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitImport
      + codeGenerator.emitHeader1("test")
      + codeGenerator.emitHeader2("test")
      + codeGenerator.emitFooter)
    val expectedCode = cleanString(
      """
        |import org.apache.spark._
        |import org.apache.spark.streaming._
        |import dbis.pig.backends.{SchemaClass, Record}
        |import dbis.pig.backends.spark._
        |
        |object SECONDS {
        |  def apply(p: Long) = Seconds(p)
        |}
        |object MINUTES {
        |  def apply(p: Long) = Minutes(p)
        |}
        |
        |object test {
        |    def main(args: Array[String]) {
        |      val conf = new SparkConf().setAppName("test_App")
        |      val ssc = new StreamingContext(conf, Seconds(1))
        |      ssc.start()
        |      ssc.awaitTermination()
        |   }
        |}
      """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for receiving a stream" in {
    val ops = parseScript(
      """
        |in = SOCKET_READ 'localhost:5555' USING PigStream(',') AS (s1: chararray, s2: int);
        |DUMP in;
      """.stripMargin, LanguageFeature.StreamingPig)

    val plan = new DataflowPlan(ops)
    val rewrittenPlan = processPlan(plan)
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(rewrittenPlan.findOperatorForAlias("in").get))
    val expectedCode = cleanString(
    """
      |val in = PigStream[_t1_Tuple]().receiveStream(ssc, "localhost", 5555,
      |(data: Array[String]) => _t1_Tuple(data(0).toString, data(1).toInt), ",")
    """.stripMargin
    )
    assert(generatedCode == expectedCode)
  }

  it should "contain code for loading a stream" in {
    val ops = parseScript(
      """
        |in = LOAD 'file.csv' USING PigStream(',') AS (f1: int, f2: int);
        |DUMP in;
      """.stripMargin, LanguageFeature.StreamingPig)

    val plan = new DataflowPlan(ops)
    val rewrittenPlan = processPlan(plan)
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(rewrittenPlan.findOperatorForAlias("in").get))
    val expectedCode = cleanString(
      """
        |val in = PigStream[_t1_Tuple]().loadStream(ssc, "file.csv",
        |(data: Array[String]) => _t1_Tuple(data(0).toInt, data(1).toInt), ",")
      """.stripMargin
    )
    assert(generatedCode == expectedCode)
  }

  it should "contain code for simple ORDER BY" in {
    val op = OrderBy(Pipe("B"), Pipe("A"), List(OrderBySpec(PositionalField(0), OrderByDirection.AscendingOrder)))
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val B = A.transform(rdd => rdd.keyBy(t => t.get(0)).sortByKey(true).map{case (k,v) => v})")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for complex ORDER BY" in {
    val op = OrderBy(Pipe("B"), Pipe("A"), List(OrderBySpec(NamedField("f1"), OrderByDirection.AscendingOrder),
      OrderBySpec(NamedField("f3"), OrderByDirection.AscendingOrder)))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))))
    op.schema = Some(schema)
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val B = A.transform(rdd => rdd.keyBy(t => custKey_B_A(t._0,t._2)).sortByKey(true).map{case (k,v) => v})")
    assert(generatedCode == expectedCode)

    val generatedHelperCode = cleanString(codeGenerator.emitHelperClass(op))
    val expectedHelperCode = cleanString(
      """case class custKey_B_A(c1: String, c2: Int) extends Ordered[custKey_B_A] {
        |def compare(that: custKey_B_A) = {
        |if (this.c1 == that.c1) { this.c2 compare that.c2 } else this.c1 compare that.c1 } }
        |""".stripMargin)
    assert(generatedHelperCode == expectedHelperCode)
  }

  it should "contain code for WINDOW with RANGE size and RANGE slider" in {
    val op = Window(Pipe("b"), Pipe("a"), (5, "SECONDS"), (1, "SECONDS"))
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val b = a.window(SECONDS(5), SECONDS(1))")
    assert(generatedCode == expectedCode)
  }

  /*
   * Not supported in Spark Streaming.
   *
  it should "contain code for WINDOW with RANGE size and ROWS slider" in {
    val op = Window(Pipe("b"), Pipe("a"), (5, "SECONDS"), (10, ""))
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val b = a.window(Time.of(5, TimeUnit.SECONDS)).every(Count.of(10))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for WINDOW with ROWS size and RANGE slider" in {
    val op = Window(Pipe("b"), Pipe("a"), (100, ""), (1, "SECONDS"))
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val b = a.window(Count.of(100)).every(Time.of(1, TimeUnit.SECONDS))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for WINDOW with ROWS size and ROWS slider" in {
    val op = Window(Pipe("b"), Pipe("a"), (100, ""), (10, ""))
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val b = a.window(Count.of(100)).every(Count.of(10))")
    assert(generatedCode == expectedCode)
  }
  */

  /*
   * Not supported in Spark Streaming.
   *
  it should "contain code for a CROSS operator on two relations" in {
    // a = Cross b, c;
    val op = Cross(Pipe("a"), List(Pipe("b"), Pipe("c")),(10, "SECONDS"))
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
                                     |val a = b.cross(c).onWindow(10, TimeUnit.SECONDS).map{
                                     |t => t._1 ++ t._2
                                     |}""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a CROSS operator on more than two relations" in {
    // a = Cross b, c, d;
    val op = Cross(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")),(10, "SECONDS"))
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
                                     |val a = b.cross(c).onWindow(10, TimeUnit.SECONDS).map{
                                     |t => t._1 ++ t._2
                                     |}.cross(d).onWindow(10, TimeUnit.SECONDS).map{
                                     |t => t._1 ++ t._2
                                     |}""".stripMargin)
    assert(generatedCode == expectedCode)
  }
  */
  it should "contain code for a binary JOIN statement with simple expression" in {
    Schema.init()
    val file = new java.net.URI("input/file.csv")
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c")), List(List(PositionalField(0)), List(PositionalField(0))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))))
    val input1 = Pipe("b",Load(Pipe("b"), file, Some(schema), Some("PigStream"), List("\",\"")))
    val input2 = Pipe("c",Load(Pipe("c"), file, Some(schema), Some("PigStream"), List("\",\"")))
    op.inputs = List(input1, input2)
    op.constructSchema

    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
                                     |val b_kv = b.map(t => (t._0,t))
                                     |val c_kv = c.map(t => (t._0,t))
                                     |val a = b_kv.join(c_kv).map{case (k,(v,w)) => _t2_Tuple(v._0, v._1, v._2, w._0, w._1, w._2)}""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a binary JOIN statement with expression lists" in {
    Schema.init()
    val file = new java.net.URI("input/file.csv")
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c")), List(List(PositionalField(0), PositionalField(1)),
      List(PositionalField(1), PositionalField(2))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))))
    val input1 = Pipe("b",Load(Pipe("b"), file, Some(schema), Some("PigStream"), List("\",\"")))
    val input2 = Pipe("c",Load(Pipe("c"), file, Some(schema), Some("PigStream"), List("\",\"")))
    op.inputs = List(input1, input2)
    op.constructSchema
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
                                     |val b_kv = b.map(t => (Array(t._0,t._1).mkString,t))
                                     |val c_kv = c.map(t => (Array(t._1,t._2).mkString,t))
                                     |val a = b_kv.join(c_kv).map{case (k,(v,w)) => _t2_Tuple(v._0, v._1, v._2, w._0, w._1, w._2)}""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a multiway JOIN statement" in {
    Schema.init()
    val file = new java.net.URI("input/file.csv")
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")), List(List(PositionalField(0)),
      List(PositionalField(0)), List(PositionalField(0))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))))
    val input1 = Pipe("b",Load(Pipe("b"), file, Some(schema), Some("PigStream"), List("\",\"")))
    val input2 = Pipe("c",Load(Pipe("c"), file, Some(schema), Some("PigStream"), List("\",\"")))
    val input3 = Pipe("d",Load(Pipe("d"), file, Some(schema), Some("PigStream"), List("\",\"")))
    op.inputs = List(input1, input2, input3)
    op.constructSchema
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
                                     |val b_kv = b.map(t => (t._0,t))
                                     |val c_kv = c.map(t => (t._0,t))
                                     |val d_kv = d.map(t => (t._0,t))
                                     |val a = b_kv.join(c_kv).join(d_kv).map{case (k,((v1,v2),v3)) => _t2_Tuple(v1._0, v1._1, v1._2, v2._0, v2._1, v2._2, v3._0, v3._1, v3._2)}""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a UNION operator on two relations" in {
    // a = UNION b, c;
    val op = Union(Pipe("a"), List(Pipe("b"), Pipe("c")))
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.union(c)")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a UNION operator on more than two relations" in {
    // a = UNION b, c, d;
    val op = Union(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")))
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.union(c).union(d)")
    assert(generatedCode == expectedCode)
  }

  /*------------------------------------------------------------------------------------------------- */
  /*                                 Testing of Hybrid Operators                                      */
  /*------------------------------------------------------------------------------------------------- */

  /*******************/
  /* Test for FILTER */
  /*******************/
  it should "contain code for FILTER" in {
    val op = Filter(Pipe("a"), Pipe("b"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.filter(t => {t.get(1) < 42})")
    assert(generatedCode == expectedCode)
  }

}