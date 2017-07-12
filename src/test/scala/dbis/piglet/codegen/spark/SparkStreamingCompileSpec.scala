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
package dbis.piglet.codegen.spark

import dbis.piglet.backends.BackendManager
import dbis.piglet.codegen.scala_lang.JoinEmitter
import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenTarget}
import dbis.piglet.expr._
import dbis.piglet.op._
import dbis.piglet.parser.PigParser.parseScript
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.plan.rewriting.Rewriter._
import dbis.piglet.plan.rewriting.Rules
import dbis.piglet.schema._
import dbis.piglet.tools.CodeMatchers
import dbis.piglet.tools.TestTools.strToUri
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SparkStreamingCompileSpec extends FlatSpec with BeforeAndAfterAll with Matchers with CodeMatchers {

  override def beforeAll() {
    Rules.registerAllRules()
  }

  def cleanString(s: String): String = s.stripLineEnd.replaceAll("""\s+""", " ").trim

  val codeGenerator = new SparkStreamingCodeGenStrategy()
  val backendConf = BackendManager.init("sparks")

  "The compiler output" should "contain the Spark Streaming header & footer" in {
    val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)

    val generatedCode = cleanString(codeGenerator.emitImport(ctx)
      + codeGenerator.emitHeader1(ctx, "test")
      + codeGenerator.emitHeader2(ctx, "test")
      + codeGenerator.emitFooter(ctx, new DataflowPlan(List.empty[PigOperator])))
    val expectedCode = cleanString(
      """
        |import org.apache.spark._
        |import org.apache.spark.streaming._
        |import dbis.piglet.backends.{SchemaClass, Record}
        |import dbis.piglet.tools._
        |import dbis.piglet.backends.spark._
        |
        |object SECONDS {
        |  def apply(p: Long) = Seconds(p)
        |}
        |object MINUTES {
        |  def apply(p: Long) = Minutes(p)
        |}
        |
        |object test {
        |    SparkStream.setAppName("test_App")
        |    val ssc = SparkStream.ssc
        |
        |    def main(args: Array[String]) {
        |
        |      ssc.start()
        |      ssc.awaitTermination()
        |   }
        |}
      """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for receiving a stream" in {
    val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    val ops = parseScript(
      """
        |in = SOCKET_READ 'localhost:5555' USING PigStream(',') AS (s1: chararray, s2: int);
        |DUMP in;
      """.stripMargin)

    val plan = new DataflowPlan(ops)
    val rewrittenPlan = rewritePlan(plan)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, rewrittenPlan.findOperatorForAlias("in").get))
    val expectedCode = cleanString(
      """
      |val in = PigStream[_t1_Tuple]().receiveStream(ssc, "localhost", 5555,
      |(data: Array[String]) => _t1_Tuple(data(0).toString, data(1).toInt), ",")
    """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for loading a stream" in {
    val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    val ops = parseScript(
      """
        |in = LOAD 'file.csv' USING PigStream(',') AS (f1: int, f2: int);
        |DUMP in;
      """.stripMargin)

    val plan = new DataflowPlan(ops)
    val rewrittenPlan = rewritePlan(plan)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, rewrittenPlan.findOperatorForAlias("in").get))
    val expectedCode = cleanString(
      """
        |val in = PigStream[_t1_Tuple]().loadStream(ssc, "file.csv",
        |(data: Array[String]) => _t1_Tuple(data(0).toInt, data(1).toInt), ",")
      """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for simple ORDER BY" in {
    val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    ctx.set("tuplePrefix", "t")
    val op = OrderBy(Pipe("B"), Pipe("A"), List(OrderBySpec(PositionalField(0), OrderByDirection.AscendingOrder)))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val B = A.transform(rdd => rdd.repartition(1).sortBy(t => t.get(0), true))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for complex ORDER BY" in {
    val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    ctx.set("tuplePrefix", "t")
    val op = OrderBy(Pipe("B"), Pipe("A"), List(OrderBySpec(NamedField("f1"), OrderByDirection.AscendingOrder),
      OrderBySpec(NamedField("f3"), OrderByDirection.AscendingOrder)))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))))
    op.schema = Some(schema)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val B = A.transform(rdd => rdd.repartition(1).sortBy(t => custKey_B_A(t._0,t._2), true))")
    assert(generatedCode == expectedCode)

    val generatedHelperCode = cleanString(codeGenerator.emitHelperClass(ctx, op))
    val expectedHelperCode = cleanString(
      """case class custKey_B_A(c1: String, c2: Int) extends Ordered[custKey_B_A] {
        |def compare(that: custKey_B_A) = {
        |if (this.c1 == that.c1) { this.c2 compare that.c2 } else this.c1 compare that.c1 } }
        |""".stripMargin)
    assert(generatedHelperCode == expectedHelperCode)
  }

  it should "contain code for WINDOW with RANGE size and RANGE slider" in {
    val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    val op = Window(Pipe("b"), Pipe("a"), (5, "SECONDS"), (1, "SECONDS"))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val b = a.window(SECONDS(5), SECONDS(1))")
    assert(generatedCode == expectedCode)
  }

  /*
   * Not supported in Spark Streaming.
   *
  it should "contain code for WINDOW with RANGE size and ROWS slider" in {
  	val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    val op = Window(Pipe("b"), Pipe("a"), (5, "SECONDS"), (10, ""))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val b = a.window(Time.of(5, TimeUnit.SECONDS)).every(Count.of(10))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for WINDOW with ROWS size and RANGE slider" in {
  	val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    val op = Window(Pipe("b"), Pipe("a"), (100, ""), (1, "SECONDS"))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val b = a.window(Count.of(100)).every(Time.of(1, TimeUnit.SECONDS))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for WINDOW with ROWS size and ROWS slider" in {
  	val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    val op = Window(Pipe("b"), Pipe("a"), (100, ""), (10, ""))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val b = a.window(Count.of(100)).every(Count.of(10))")
    assert(generatedCode == expectedCode)
  }
  */

  /*
   * Not supported in Spark Streaming.
   *
  it should "contain code for a CROSS operator on two relations" in {
  	val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    // a = Cross b, c;
    val op = Cross(Pipe("a"), List(Pipe("b"), Pipe("c")),(10, "SECONDS"))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
                                     |val a = b.cross(c).onWindow(10, TimeUnit.SECONDS).map{
                                     |t => t._1 ++ t._2
                                     |}""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a CROSS operator on more than two relations" in {
  	val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    // a = Cross b, c, d;
    val op = Cross(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")),(10, "SECONDS"))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
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
    val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    ctx.set("tuplePrefix", "t")
    JoinEmitter.joinKeyVars.clear()

    Schema.init()
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c")), List(List(PositionalField(0)), List(PositionalField(0))))
    val schema = Schema(Array(Field("f1", Types.CharArrayType),
                              Field("f2", Types.DoubleType),
                              Field("f3", Types.IntType)))
    val input1 = Pipe("b", Load(Pipe("b"), "input/file.csv", Some(schema), Some("PigStream"), List("\",\"")))
    val input2 = Pipe("c", Load(Pipe("c"), "input/file.csv", Some(schema), Some("PigStream"), List("\",\"")))
    op.inputs = List(input1, input2)
    op.constructSchema

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val b_kv = b.map(t => (t._0,t))
      |val c_kv = c.map(t => (t._0,t))
      |val a = b_kv.join(c_kv).map{case (k,(v,w)) => _t$1_Tuple(v._0, v._1, v._2, w._0, w._1, w._2)}""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a binary JOIN statement with expression lists" in {
    val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    ctx.set("tuplePrefix", "t")
    JoinEmitter.joinKeyVars.clear()
    
    Schema.init()
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c")), List(List(PositionalField(0), PositionalField(1)),
      List(PositionalField(1), PositionalField(2))))
    val schema = Schema(Array(Field("f1", Types.CharArrayType),
                              Field("f2", Types.DoubleType),
                              Field("f3", Types.IntType)))
    val input1 = Pipe("b", Load(Pipe("b"), "input/file.csv", Some(schema), Some("PigStream"), List("\",\"")))
    val input2 = Pipe("c", Load(Pipe("c"), "input/file.csv", Some(schema), Some("PigStream"), List("\",\"")))
    op.inputs = List(input1, input2)
    op.constructSchema
        
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val b_kv = b.map(t => (Array(t._0,t._1).mkString,t))
      |val c_kv = c.map(t => (Array(t._1,t._2).mkString,t))
      |val a = b_kv.join(c_kv).map{case (k,(v,w)) => _t$1_Tuple(v._0, v._1, v._2, w._0, w._1, w._2)}""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a multiway JOIN statement" in {
    val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    ctx.set("tuplePrefix", "t")
    JoinEmitter.joinKeyVars.clear()

    Schema.init()
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")), List(List(PositionalField(0)),
      List(PositionalField(0)), List(PositionalField(0))))
    val schema = Schema(Array(Field("f1", Types.CharArrayType),
                              Field("f2", Types.DoubleType),
                              Field("f3", Types.IntType)))
    val input1 = Pipe("b", Load(Pipe("b"), "input/file.csv", Some(schema), Some("PigStream"), List("\",\"")))
    val input2 = Pipe("c", Load(Pipe("c"), "input/file.csv", Some(schema), Some("PigStream"), List("\",\"")))
    val input3 = Pipe("d", Load(Pipe("d"), "input/file.csv", Some(schema), Some("PigStream"), List("\",\"")))
    op.inputs = List(input1, input2, input3)
    op.constructSchema
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
                                     |val b_kv = b.map(t => (t._0,t))
                                     |val c_kv = c.map(t => (t._0,t))
                                     |val d_kv = d.map(t => (t._0,t))
                                     |val a = b_kv.join(c_kv).join(d_kv).map{case (k,((v1,v2),v3)) => _t$1_Tuple(v1._0, v1._1, v1._2, v2._0, v2._1, v2._2, v3._0, v3._1, v3._2)}""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a UNION operator on two relations" in {
    val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    // a = UNION b, c;
    val op = Union(Pipe("a"), List(Pipe("b"), Pipe("c")))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val a = b.union(c)")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a UNION operator on more than two relations" in {
    val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    // a = UNION b, c, d;
    val op = Union(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
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
    val ctx = CodeGenContext(CodeGenTarget.SparkStreaming)
    CodeEmitter.profiling = false

    val op = Filter(Pipe("a"), Pipe("b"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val a = b.filter{t => t.get(1) < 42}")
    assert(generatedCode == expectedCode)
  }

}
