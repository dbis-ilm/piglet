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
package dbis.piglet.codegen.flink

import dbis.piglet.Piglet._
import dbis.piglet.op._
import dbis.piglet.expr._
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.schema._
import org.scalatest.FlatSpec
import java.net.URI
import dbis.piglet.tools.Conf
import dbis.piglet.backends.BackendManager
import dbis.piglet.tools.logging.PigletLogging
import dbis.piglet.tools.CodeMatchers
import org.scalatest.{ Matchers, BeforeAndAfterAll, FlatSpec }
import dbis.piglet.parser.PigParser.parseScript
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.codegen.CodeGenTarget

class FlinkStreamingCompileSpec extends FlatSpec with BeforeAndAfterAll with Matchers with CodeMatchers with PigletLogging {

  def cleanString(s: String): String = s.stripLineEnd.replaceAll("""\s+""", " ").trim

  val backendConf = BackendManager.init("flinks")
  val codeGenerator = new FlinkStreamingCodeGenStrategy()

  /**************************************/
  /* Test for IMPORT, HEADER and FOOTER */
  /**************************************/
  "The compiler output" should "contain the Flink header & footer" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val generatedCode = cleanString(codeGenerator.emitImport(ctx)
      + codeGenerator.emitHeader1(ctx, "test")
      + codeGenerator.emitHeader2(ctx, "test")
      + codeGenerator.emitFooter(ctx, new DataflowPlan(List.empty[PigOperator])))
    val expectedCode = cleanString("""
      |import org.apache.flink.streaming.api.scala._
      |import dbis.piglet.backends.flink._
      |import dbis.piglet.backends.flink.streaming._
      |import java.util.concurrent.TimeUnit
      |import org.apache.flink.util.Collector
      |import org.apache.flink.streaming.api.windowing.assigners._
      |import org.apache.flink.streaming.api.windowing.evictors._
      |import org.apache.flink.streaming.api.windowing.time._
      |import org.apache.flink.streaming.api.windowing.triggers._
      |import org.apache.flink.streaming.api.windowing.windows._
      |import org.apache.flink.streaming.api.TimeCharacteristic
      |import dbis.piglet.backends.{SchemaClass, Record}
      |
      |object test {
      |    val env = StreamExecutionEnvironment.getExecutionEnvironment
      |    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
      |    def main(args: Array[String]) {
      |        env.execute("Starting Query")
      |    }
      |}
    """.stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  /*------------------------------------------------------------------------------------------------- */
  /*                               Testing of Connecting Operators                                    */
  /*------------------------------------------------------------------------------------------------- */

  /*****************/
  /* Test for DUMP */
  /*****************/
  it should "contain code for DUMP" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val op = Dump(Pipe("a"))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""a.map(_.mkString()).print""")
    generatedCode should matchSnippet(expectedCode)
  }

  /******************/
  /* Tests for LOAD */
  /******************/
  it should "contain code for LOAD" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val file = new URI(new java.io.File(".").getCanonicalPath + "/input/file.csv")

    val op = Load(Pipe("a"), file)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString(s"""val a = PigStream[Record]().loadStream(env, "$file", (data: Array[String]) => Record(data))""")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for LOAD with PigStream" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val file = new URI(new java.io.File(".").getCanonicalPath + "/input/file.csv")
    val op = Load(Pipe("a"), file, None, Some("PigStream"), List("""','"""))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString(s"""val a = PigStream[Record]().loadStream(env, "$file", (data: Array[String]) => Record(data), ',')""")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for LOAD with RDFStream" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val file = new URI(new java.io.File(".").getCanonicalPath + "/file.n3")
    val op = Load(Pipe("a"), file, None, Some("RDFStream"))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString(s"""val a = RDFStream[Record]().loadStream(env, "${file}", (data: Array[String]) => Record(data))""")
    generatedCode should matchSnippet(expectedCode)
  }

  /*************************/
  /* Tests for SOCKET_READ */
  /*************************/
  it should "contain code for SOCKET_READ" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val schema = new Schema(BagType(TupleType(Array(Field("line", Types.CharArrayType)))), "t1")
    val op = SocketRead(Pipe("a"), SocketAddress("", "localhost", "9999"), "")
    op.schema = Some(schema)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val a = PigStream[_t$1_Tuple]().connect(env, "localhost", 9999, (data: Array[String]) => _t$1_Tuple(data(0).toString))
      """.stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for SOCKET_READ with PigStream" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType), Field("f2", Types.DoubleType)))), "t1")
    val op = SocketRead(Pipe("a"), SocketAddress("", "localhost", "9999"), "", None, Some("PigStream"), List("""','"""))
    op.schema = Some(schema)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val a = PigStream[_t$1_Tuple]().connect(env, "localhost", 9999, (data: Array[String]) => _t$1_Tuple(data(0).toString, data(1).toDouble), ',')
      """.stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for SOCKET_READ with RDFStream" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType), Field("f2", Types.DoubleType)))), "t1")
    val op = SocketRead(Pipe("a"), SocketAddress("", "localhost", "9999"), "", None, Some("RDFStream"))
    op.schema = Some(schema)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val a = RDFStream[_t$1_Tuple]().connect(env, "localhost", 9999, (data: Array[String]) => _t$1_Tuple(data(0).toString, data(1).toDouble))
      """.stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for SOCKET_READ in ZMQ mode" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val schema = new Schema(BagType(TupleType(Array(Field("line", Types.CharArrayType)))), "t1")
    val op = SocketRead(Pipe("a"), SocketAddress("tcp://", "localhost", "9999"), "zmq")
    op.schema = Some(schema)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val a = PigStream[_t$1_Tuple]().zmqSubscribe(env, "tcp://localhost:9999", (data: Array[String]) => _t$1_Tuple(data(0).toString))
      """.stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  /**************************/
  /* Tests for SOCKET_WRITE */
  /**************************/
  it should "contain code for SOCKET_WRITE using a Web-Socket" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val schema = new Schema(BagType(TupleType(Array(Field("line", Types.CharArrayType)))), "t1")
    val op = SocketWrite(Pipe("a"), SocketAddress("", "localhost", "9999"), "")
    op.schema = Some(schema)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""PigStream[_t$1_Tuple]().bind("localhost", 9999, a)""")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for SOCKET_WRITE in ZMQ mode" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val schema = new Schema(BagType(TupleType(Array(Field("line", Types.CharArrayType)))), "t1")
    val op = SocketWrite(Pipe("a"), SocketAddress("tcp://", "localhost", "9999"), "zmq")
    op.schema = Some(schema)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""PigStream[_t$1_Tuple]().zmqPublish("tcp://localhost:9999", a)""")
    generatedCode should matchSnippet(expectedCode)
  }

  /*******************/
  /* Tests for STORE */
  /*******************/
  it should "contain code for STORE" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val file = new URI(new java.io.File(".").getCanonicalPath + "/input/file.csv")
    val op = Store(Pipe("A"), file)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString(s"""PigStream[Record]().writeStream("${file}", A)""")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for STORE with a known schema" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    Schema.init()
    val file = new URI(new java.io.File(".").getCanonicalPath + "/input/file.csv")
    val op = Store(Pipe("A"), file)
    op.schema = Some(Schema(Array(
      Field("f1", Types.IntType),
      Field("f2", BagType(TupleType(Array(Field("f3", Types.DoubleType), Field("f4", Types.DoubleType))))))))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""PigStream[_t$1_Tuple]().writeStream("""" + file + """", A)""")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for STORE with delimiter" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    Schema.init()
    val file = new URI(new java.io.File(".").getCanonicalPath + "/input/file.csv")
    val op = Store(Pipe("A"), file, Some("PigStream"), List(""""#""""))
    op.schema = Some(Schema(Array(
      Field("f1", Types.IntType),
      Field("f2", BagType(TupleType(Array(Field("f3", Types.DoubleType), Field("f4", Types.DoubleType))))))))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""PigStream[_t$1_Tuple]().writeStream("""" + file + """", A, "#")""")
    generatedCode should matchSnippet(expectedCode)
  }

  /*------------------------------------------------------------------------------------------------- */
  /*                                 Testing of Window Operators                                      */
  /*------------------------------------------------------------------------------------------------- */

  /*********************/
  /* Test for DISTINCT */
  /*********************/
  it should "contain code for DISTINCT" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType), Field("f2", Types.DoubleType)))), "t1")
    val op = Distinct(Pipe("c"), Pipe("b"), true)
    op.schema = Some(schema)

    // Helping surrounding operators
    val win = Window(Pipe("b"), Pipe("a"), (5, "SECONDS"), (1, "SECONDS"))
    win.schema = Some(schema)
    val sink = Dump(Pipe("c"))
    sink.schema = Some(schema)
    val apply = WindowApply(Pipe("d"), Pipe("b"), "c")
    apply.schema = Some(schema)

    val pipeB = Pipe("b", win)
    val pipeC = Pipe("c", op)
    val pipeD = Pipe("d", apply)

    win.outputs = List(pipeB)
    apply.inputs = List(pipeB)
    apply.outputs = List(pipeD)
    op.inputs = List(pipeB)
    op.outputs = List(pipeC)
    sink.inputs = List(pipeD, pipeC)

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("")
    generatedCode should matchSnippet(expectedCode)

    val generatedHelperCode = cleanString(codeGenerator.emitHelperClass(ctx, apply))
    val expectedHelperCode = cleanString("""
      |def WindowFuncc(wi: Window, ts: Iterable[_t$1_Tuple], out: Collector[_t$1_Tuple]) = { 
      |  ts
      |  .toList.distinct
      |  .foreach { t => out.collect((t)) } 
      |}""".stripMargin)
    generatedHelperCode should matchSnippet(expectedHelperCode)
  }

  /*********************/
  /* Test for ORDER BY */
  /*********************/

  it should "contain code for simple ORDER BY" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType), Field("f2", Types.DoubleType)))), "t1")
    val op = OrderBy(Pipe("c"), Pipe("b"), List(OrderBySpec(PositionalField(0), OrderByDirection.AscendingOrder)), true)
    op.schema = Some(schema)

    // Helping surrounding operators
    val win = Window(Pipe("b"), Pipe("a"), (5, "SECONDS"), (1, "SECONDS"))
    win.schema = Some(schema)
    val sink = Dump(Pipe("c"))
    sink.schema = Some(schema)
    val apply = WindowApply(Pipe("d"), Pipe("b"), "c")
    apply.schema = Some(schema)

    val pipeB = Pipe("b", win)
    val pipeC = Pipe("c", op)
    val pipeD = Pipe("d", apply)

    win.outputs = List(pipeB)
    apply.inputs = List(pipeB)
    apply.outputs = List(pipeD)
    op.inputs = List(pipeB)
    op.outputs = List(pipeC)
    sink.inputs = List(pipeD, pipeC)

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("")
    generatedCode should matchSnippet(expectedCode)

    val generatedHelperCode = cleanString(codeGenerator.emitHelperClass(ctx, apply))
    val expectedHelperCode = cleanString("""
      |def WindowFuncc(wi: Window, ts: Iterable[_t$1_Tuple], out: Collector[_t$1_Tuple]) = {
      |  ts
      |  .toList.sortBy(t => t._0)(Ordering.String)
      |  .foreach { t => out.collect((t)) }
      |}""".stripMargin)
    generatedHelperCode should matchSnippet(expectedHelperCode)
  }

  it should "contain code for complex ORDER BY" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))), "t1")
    val op = OrderBy(Pipe("c"), Pipe("b"), List(OrderBySpec(NamedField("f1"), OrderByDirection.AscendingOrder),
      OrderBySpec(NamedField("f3"), OrderByDirection.AscendingOrder)), true)
    op.schema = Some(schema)

    // Helping surrounding operators
    val win = Window(Pipe("b"), Pipe("a"), (5, "SECONDS"), (1, "SECONDS"))
    win.schema = Some(schema)
    val sink = Dump(Pipe("c"))
    sink.schema = Some(schema)
    val apply = WindowApply(Pipe("d"), Pipe("b"), "c")
    apply.schema = Some(schema)

    val pipeB = Pipe("b", win)
    val pipeC = Pipe("c", op)
    val pipeD = Pipe("d", apply)

    win.outputs = List(pipeB)
    apply.inputs = List(pipeB)
    apply.outputs = List(pipeD)
    op.inputs = List(pipeB)
    op.outputs = List(pipeC)
    sink.inputs = List(pipeD, pipeC)

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("")
    generatedCode should matchSnippet(expectedCode)

    val generatedHelperCode = cleanString(codeGenerator.emitHelperClass(ctx, apply))
    val expectedHelperCode = cleanString("""
      |def WindowFuncc(wi: Window, ts: Iterable[_t$1_Tuple], out: Collector[_t$1_Tuple]) = {
      |  ts
      |  .toList.sortBy(t => (t._0,t._2))(Ordering.Tuple2(Ordering.String,Ordering.Int))
      |  .foreach { t => out.collect((t)) }
      |}""".stripMargin)
    generatedHelperCode should matchSnippet(expectedHelperCode)
  }

  /******************************/
  /* Tests for WINDOW (unkeyed) */
  /******************************/

  it should "contain code for WINDOW with RANGE size and RANGE slider" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val op = Window(Pipe("b"), Pipe("a"), (5, "SECONDS"), (1, "SECONDS"))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val b = a.timeWindowAll(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS))")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for WINDOW with RANGE size and ROWS slider" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val op = Window(Pipe("b"), Pipe("a"), (5, "SECONDS"), (10, ""))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val b = a.windowAll(TumblingTimeWindows.of(Time.of(5, TimeUnit.SECONDS))).trigger(CountTrigger.of(10))")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for WINDOW with ROWS size and RANGE slider" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val op = Window(Pipe("b"), Pipe("a"), (100, ""), (1, "SECONDS"))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val b = a.windowAll(GlobalWindows.create()).trigger(ContinuousEventTimeTrigger.of(Time.of(1, TimeUnit.SECONDS))).evictor(CountEvictor.of(100))")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for WINDOW with ROWS size and ROWS slider" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val op = Window(Pipe("b"), Pipe("a"), (100, ""), (10, ""))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val b = a.countWindowAll(100, 10)")
    generatedCode should matchSnippet(expectedCode)
  }

  /*------------------------------------------------------------------------------------------------- */
  /*                                Testing of Unifying Operators                                     */
  /*------------------------------------------------------------------------------------------------- */

  /*******************/
  /* Tests for CROSS */
  /*******************/
  it should "contain code for a CROSS operator on two relations" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    // a = Cross b, c;
    val file = new java.net.URI("input/file.csv")
    val op = Cross(Pipe("a"), List(Pipe("b"), Pipe("c")), (10, "SECONDS"))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType), Field("f2", Types.DoubleType)))), "t1")
    op.schema = Some(schema)
    val input1 = Pipe("b", Load(Pipe("b"), file, Some(schema), Some("PigStream"), List("\",\"")))
    val input2 = Pipe("c", Load(Pipe("c"), file, Some(schema), Some("PigStream"), List("\",\"")))
    op.inputs = List(input1, input2)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val a = b.map(t => (1,t)).join(c.map(t => (1,t))).where(t => t._1).equalTo(t => t._1)
      |.window(TumblingTimeWindows.of(Time.SECONDS(10))).apply{(t1,t2) => (t1._2, t2._2)} 
      |.map{ v => v match {case (v,w) => _t$1_Tuple(v._0, v._1) }}
      |""".stripMargin.replaceAll("\n", ""))
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a CROSS operator on more than two relations" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    // a = Cross b, c, d;
    val op = Cross(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")), (10, "SECONDS"))
    val file = new java.net.URI("input/file.csv")
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType), Field("f2", Types.DoubleType)))), "t1")
    op.schema = Some(schema)
    val input1 = Pipe("b", Load(Pipe("b"), file, Some(schema), Some("PigStream"), List("\",\"")))
    val input2 = Pipe("c", Load(Pipe("c"), file, Some(schema), Some("PigStream"), List("\",\"")))
    val input3 = Pipe("d", Load(Pipe("d"), file, Some(schema), Some("PigStream"), List("\",\"")))
    op.inputs = List(input1, input2, input3)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val a = b.map(t => (1,t)).join(c.map(t => (1,t))).where(t => t._1).equalTo(t => t._1)
      |.window(TumblingTimeWindows.of(Time.SECONDS(10))).apply{(t1,t2) => (t1._2, t2._2)} 
      |.map(t => (1,t)).join(d.map(t => (1,t))).where(t => t._1).equalTo(t => t._1)
      |.window(TumblingTimeWindows.of(Time.SECONDS(10))).apply{(t1,t2) => (t1._2, t2._2)} 
      |.map{ v => v match {case ((v1,v2),v3) => _t$1_Tuple(v1._0, v1._1, v2._0, v2._1, v3._0, v3._1) }}
      |""".stripMargin.replaceAll("\n", ""))
    generatedCode should matchSnippet(expectedCode)
  }

  /******************/
  /* Tests for JOIN */
  /******************/
  it should "contain code for a binary JOIN statement with simple expression" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val file = new java.net.URI("input/file.csv")
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c")), List(List(PositionalField(0)), List(PositionalField(0))), (5, "SECONDS"))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))), "t1")
    op.schema = Some(schema)
    val input1 = Pipe("b", Load(Pipe("b"), file, Some(schema), Some("PigStream"), List("\",\"")))
    val input2 = Pipe("c", Load(Pipe("c"), file, Some(schema), Some("PigStream"), List("\",\"")))
    op.inputs = List(input1, input2)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
        |val a = b.join(c).where(k => k match {case t => t._0}).equalTo(t => t._0)
        |.window(TumblingTimeWindows.of(Time.seconds(5))).apply{(t1,t2) => (t1, t2)} 
        |.map{ v => v match {case (v,w) => _t$1_Tuple(v._0, v._1, v._2) }} 
        |""".stripMargin.replaceAll("\n", ""))
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a binary JOIN statement with expression lists" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val file = new java.net.URI("input/file.csv")
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c")), List(List(PositionalField(0), PositionalField(1)),
      List(PositionalField(1), PositionalField(2))), (5, "SECONDS"))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))), "t1")
    op.schema = Some(schema)
    val input1 = Pipe("b", Load(Pipe("b"), file, Some(schema), Some("PigStream"), List("\",\"")))
    val input2 = Pipe("c", Load(Pipe("c"), file, Some(schema), Some("PigStream"), List("\",\"")))
    op.inputs = List(input1, input2)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
        |val a = b.join(c).where(k => k match {case t => Array(t._0,t._1).mkString}).equalTo(t => Array(t._1,t._2).mkString)
        |.window(TumblingTimeWindows.of(Time.seconds(5))).apply{(t1,t2) => (t1, t2)} 
        |.map{ v => v match {case (v,w) => _t$1_Tuple(v._0, v._1, v._2) }}
        |""".stripMargin.replaceAll("\n", ""))

    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a multiway JOIN statement" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val file = new java.net.URI("input/file.csv")
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")), List(List(PositionalField(0)),
      List(PositionalField(0)), List(PositionalField(0))), (5, "SECONDS"))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))), "t1")
    op.schema = Some(schema)
    val input1 = Pipe("b", Load(Pipe("b"), file, Some(schema), Some("PigStream"), List("\",\"")))
    val input2 = Pipe("c", Load(Pipe("c"), file, Some(schema), Some("PigStream"), List("\",\"")))
    val input3 = Pipe("d", Load(Pipe("d"), file, Some(schema), Some("PigStream"), List("\",\"")))
    op.inputs = List(input1, input2, input3)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val a = b.join(c).where(k => k match {case t => t._0}).equalTo(t => t._0)
      |.window(TumblingTimeWindows.of(Time.seconds(5))).apply{(t1,t2) => (t1, t2)} 
      |.join(d).where(k => k match {case (t, m0) => t._0}).equalTo(t => t._0)
      |.window(TumblingTimeWindows.of(Time.seconds(5))).apply{(t1,t2) => (t1, t2)} 
      |.map{ v => v match {case ((v1,v2),v3) => _t$1_Tuple(v1._0, v1._1, v1._2, v2._0, v2._1, v2._2, v3._0, v3._1, v3._2) }}
      |""".stripMargin.replaceAll("\n", ""))
    generatedCode should matchSnippet(expectedCode)
  }

  /*******************/
  /* Tests for UNION */
  /*******************/
  it should "contain code for a UNION operator on two relations" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    // a = UNION b, c;
    val op = Union(Pipe("a"), List(Pipe("b"), Pipe("c")))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
        |val a = b.union(c)""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a UNION operator on more than two relations" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    // a = UNION b, c, d;
    val op = Union(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
        |val a = b.union(c).union(d)""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  /*------------------------------------------------------------------------------------------------- */
  /*                                 Testing of Hybrid Operators                                      */
  /*------------------------------------------------------------------------------------------------- */

  /*******************/
  /* Test for FILTER */
  /*******************/
  it should "contain code for FILTER" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val op = Filter(Pipe("a"), Pipe("b"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType)))), "t1")
    op.schema = Some(schema)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val a = b.filter(t => {t._1 < 42})")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for FILTER in window mode" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType)))), "t1")
    val op = Filter(Pipe("c"), Pipe("b"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))), true)
    op.schema = Some(schema)

    // Helping surrounding operators
    val win = Window(Pipe("b"), Pipe("a"), (5, "SECONDS"), (1, "SECONDS"))
    win.schema = Some(schema)
    val sink = Dump(Pipe("c"))
    sink.schema = Some(schema)
    val apply = WindowApply(Pipe("d"), Pipe("b"), "c")
    apply.schema = Some(schema)

    val pipeB = Pipe("b", win)
    val pipeC = Pipe("c", op)
    val pipeD = Pipe("d", apply)

    win.outputs = List(pipeB)
    apply.inputs = List(pipeB)
    apply.outputs = List(pipeD)
    op.inputs = List(pipeB)
    op.outputs = List(pipeC)
    sink.inputs = List(pipeD, pipeC)

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("")
    generatedCode should matchSnippet(expectedCode)

    val generatedHelperCode = cleanString(codeGenerator.emitHelperClass(ctx, apply))
    val expectedHelperCode = cleanString("""
      |def WindowFuncc(wi: Window, ts: Iterable[_t$1_Tuple], out: Collector[_t$1_Tuple]) = { 
      |  ts
      |  .filter(t => {t._1 < 42})
      |  .foreach { t => out.collect((t)) } 
      |}""".stripMargin)
    generatedHelperCode should matchSnippet(expectedHelperCode)
  }

  /*********************/
  /* Tests for FOREACH */
  /*********************/
  it should "contain code for a FOREACH statement with function expressions" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    // a = FOREACH b GENERATE TOMAP("field1", $0, "field2", $1);
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType)))), "t1")
    val op = Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(
      GeneratorExpr(Func("TOMAP", List(
        RefExpr(Value("\"field1\"")),
        RefExpr(PositionalField(0)),
        RefExpr(Value("\"field2\"")),
        RefExpr(PositionalField(1))))))))
    op.schema = Some(schema)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""val a = b.map(t => _t$1_Tuple(PigFuncs.toMap("field1",t.get(0),"field2",t.get(1))))""")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a FOREACH statement with function expressions in window mode" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    // a = FOREACH b GENERATE TOMAP("field1", $0, "field2", $1);
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType)))), "t1")
    val op = Foreach(Pipe("d"), Pipe("b"), GeneratorList(List(
      GeneratorExpr(Func("TOMAP", List(
        RefExpr(Value("\"field1\"")),
        RefExpr(PositionalField(0)),
        RefExpr(Value("\"field2\"")),
        RefExpr(PositionalField(1))))))), windowMode = true)
    op.schema = Some(schema)
    // Helping surrounding operators
    val win = Window(Pipe("b"), Pipe("a"), (5, "SECONDS"), (1, "SECONDS"))
    win.schema = Some(schema)
    val apply = WindowApply(Pipe("c"), Pipe("b"), "d")
    apply.schema = Some(schema)

    val pipeB = Pipe("b", win)
    val pipeC = Pipe("c", apply)
    val pipeD = Pipe("d", op)

    win.outputs = List(pipeB)
    apply.inputs = List(pipeB)
    apply.outputs = List(pipeD)
    op.inputs = List(pipeC, pipeB)

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("")
    generatedCode should matchSnippet(expectedCode)

    val generatedHelperCode = cleanString(codeGenerator.emitHelperClass(ctx, apply))
    val expectedHelperCode = cleanString("""
      |def WindowFuncd(wi: Window, ts: Iterable[_t$1_Tuple], out: Collector[_t$1_Tuple]) = { 
      |ts
      |.foreach { t => out.collect(_t1_Tuple(PigFuncs.toMap("field1",t._0,"field2",t._1))) }
      |}""".stripMargin)
    generatedHelperCode should matchSnippet(expectedHelperCode)
  }

  it should "contain code for a FOREACH statement with another function expression" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    // a = FOREACH b GENERATE $0, COUNT($1) AS CNT;
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.LongType)))), "t1")
    val op = Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(
      GeneratorExpr(RefExpr(PositionalField(0))),
      GeneratorExpr(Func("COUNT", List(RefExpr(PositionalField(1)))), Some(Field("CNT", Types.LongType))))))
    op.schema = Some(schema)
    val file = new java.net.URI("input/file.csv")
    val input = Pipe("b", Load(Pipe("b"), file, Some(schema), Some("PigStream"), List("\",\"")))
    op.inputs = List(input)

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val a_fold = b.keyBy(t => 0).mapWithState(streamAcc_a)
      |val a = a_fold.map(t => _t$1_Tuple(t._t._0, t._0))""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for deref operator on maps in FOREACH statement" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    // a = FOREACH b GENERATE $0#"k1", $1#"k2";
    val op = Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(GeneratorExpr(RefExpr(DerefMap(PositionalField(0), "\"k1\""))),
      GeneratorExpr(RefExpr(DerefMap(PositionalField(1), "\"k2\""))))))
    op.schema = op.constructSchema
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""val a = b.map(t => _t$1_Tuple(t.get(0)("k1"), t.get(1)("k2")))""")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for deref operator on tuple in FOREACH statement" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    // a = FOREACH b GENERATE $0.$1, $2.$0;
    val op = Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(GeneratorExpr(RefExpr(DerefTuple(PositionalField(0), PositionalField(1)))),
      GeneratorExpr(RefExpr(DerefTuple(PositionalField(2), PositionalField(0)))))))
    op.schema = op.constructSchema
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val a = b.map(t => _t$1_Tuple(t.get(0).get(1), t.get(2).get(0)))")
    generatedCode should matchSnippet(expectedCode)
  }

  /**********************/
  /* Tests for GROUP BY */
  /**********************/
  it should "contain code for GROUP BY ALL" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val ops = parseScript(
      """
        |bb = LOAD 'file.csv' USING PigStream(',') AS (f1: int, f2: chararray, f3: double);
        |aa = GROUP bb ALL;
      """.stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("aa").get
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""val aa = bb.map(t => _t$1_Tuple("all", List(t))).keyBy(t => t._0)""")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for GROUP BY $0" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val ops = parseScript(
      """
        |bb = LOAD 'file.csv' USING PigStream(',') AS (f1: int, f2: chararray, f3: double);
        |aa = GROUP bb BY f1;
      """.stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("aa").get
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val aa = bb.map(t => _t$1_Tuple(t._0, List(t))).keyBy(t => t._0)")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for GROUP BY with multiple keys" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val ops = parseScript(
      """
        |bb = LOAD 'file.csv' USING PigStream(',') AS (f1: int, f2: chararray, f3: double);
        |aa = GROUP bb BY ($0, $1);
      """.stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("aa").get
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val aa = bb.map(t => _t$1_Tuple((t._0,t._1), List(t))).keyBy(t => t._0)")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for GROUP BY $0 in window mode" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType), Field("f2", Types.DoubleType)))), "t1")
    val op = Grouping(Pipe("a"), Pipe("b"), GroupingExpression(List(PositionalField(0))), true)
    op.schema = Some(schema)

    // Helping surrounding operators
    val win = Window(Pipe("b"), Pipe("a"), (5, "SECONDS"), (1, "SECONDS"))
    win.schema = Some(schema)
    val sink = Dump(Pipe("c"))
    sink.schema = Some(schema)
    val apply = WindowApply(Pipe("d"), Pipe("b"), "c")
    apply.schema = Some(schema)

    val pipeB = Pipe("b", win)
    val pipeC = Pipe("c", op)
    val pipeD = Pipe("d", apply)

    win.outputs = List(pipeB)
    apply.inputs = List(pipeB)
    apply.outputs = List(pipeD)
    op.inputs = List(pipeB)
    op.outputs = List(pipeC)
    sink.inputs = List(pipeD, pipeC)

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("")
    generatedCode should matchSnippet(expectedCode)
    val generatedHelperCode = cleanString(codeGenerator.emitHelperClass(ctx, apply))
    val expectedHelperCode = cleanString("""
      |def WindowFuncc(wi: Window, ts: Iterable[_t$1_Tuple], out: Collector[_t$1_Tuple]) = { 
      |  ts 
      |  .groupBy(t => t._0).map(t => _t$1_Tuple(t._1,t._2)) 
      |  .foreach { t => out.collect((t)) } 
      |}""".stripMargin)

    generatedHelperCode should matchSnippet(expectedHelperCode)
  }

  /****************************/
  /* Tests for STREAM THROUGH */
  /****************************/
  it should "contain code for the stream through statement without parameters" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val ops = parseScript(
      """data = load 'data.csv' as (f1: int, f2: int);
        |res = STREAM data THROUGH myOp();""".stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("res").get

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val data_helper = data.map(t => List(t._0, t._1))
      |val res = myOp(env, data_helper).map(t => _t$1_Tuple(t(0).asInstanceOf[Int], t(1).asInstanceOf[Int]))
      |""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for the stream through statement with parameters" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val ops = parseScript(
      """data = load 'data.csv' as (f1: int, f2: int);
        |res = STREAM data THROUGH package.myOp(1, 42.0);""".stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("res").get
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val data_helper = data.map(t => List(t._0, t._1))
      |val res = package.myOp(env, data_helper,1,42.0).map(t => _t$1_Tuple(t(0).asInstanceOf[Int], t(1).asInstanceOf[Int]))
      |""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  /*------------------------------------------------------------------------------------------------- */
  /*                              Testing of Stream Only Operators                                    */
  /*------------------------------------------------------------------------------------------------- */

  /************************/
  /* Tests for SPLIT INTO */
  /************************/
  /* Split is rewritten to multiple filters, so no emitter for split into
   * 
  it should "contain code for SPLIT a INTO b IF f1==2, c IF f2>3" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val ops = parseScript(
      """a = load 'data.csv' as (f1: int, f2: int);
        |SPLIT a INTO b IF f1==2, c IF f2>3;""".stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("b").get
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val b = a.filter(t => {t._0 == 2}) 
      |val c = a.filter(t => {t._1 > 3})
      |""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }
  */

  /********************/
  /* Tests for SAMPLE */
  /********************/
  it should "contain code for the SAMPLE operator with a literal value" in {
    val ctx = CodeGenContext(CodeGenTarget.FlinkStreaming)
    val op = Sample(Pipe("a"), Pipe("b"), RefExpr(Value("0.1")))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val a = b.filter(t => util.Random.nextDouble <= 0.1)
      |""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }
}

