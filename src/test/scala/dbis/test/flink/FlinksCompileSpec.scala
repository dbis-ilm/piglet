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

import dbis.pig.Piglet._
import dbis.pig.codegen.flink.FlinkStreamingCodeGen
import dbis.pig.op._
import dbis.pig.expr._
import dbis.pig.plan.DataflowPlan
import dbis.pig.schema._
import org.scalatest.FlatSpec
import java.net.URI
import dbis.pig.tools.Conf
import dbis.pig.backends.BackendManager
import dbis.pig.tools.logging.PigletLogging
import dbis.test.CodeMatchers
import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}



class FlinksCompileSpec extends FlatSpec with BeforeAndAfterAll with Matchers with CodeMatchers with PigletLogging {

  def cleanString(s: String): String = s.stripLineEnd.replaceAll("""\s+""", " ").trim

  val backendConf = BackendManager.backend("flinks")
  BackendManager.backend = backendConf
  val templateFile = backendConf.templateFile

  logger.debug(s"template file: $templateFile")

  /**************************************/
  /* Test for IMPORT, HEADER and FOOTER */
  /**************************************/
  "The compiler output" should "contain the Flink header & footer" in {
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitImport()
      + codeGenerator.emitHeader1("test")
      + codeGenerator.emitHeader2("test")
      + codeGenerator.emitFooter)
    val expectedCode = cleanString("""
      |import org.apache.flink.streaming.api.scala._
      |import dbis.pig.backends.flink._
      |import dbis.pig.backends.flink.streaming._
      |import java.util.concurrent.TimeUnit
      |import org.apache.flink.util.Collector
      |import org.apache.flink.streaming.api.windowing.assigners._
      |import org.apache.flink.streaming.api.windowing.evictors._
      |import org.apache.flink.streaming.api.windowing.time._
      |import org.apache.flink.streaming.api.windowing.triggers._
      |import org.apache.flink.streaming.api.windowing.windows._
      |import dbis.pig.backends.{SchemaClass, Record}
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

  /*------------------------------------------------------------------------------------------------- */
  /*                               Testing of Connecting Operators                                    */
  /*------------------------------------------------------------------------------------------------- */

  /*****************/
  /* Test for DUMP */
  /*****************/
  it should "contain code for DUMP" in {
    val op = Dump(Pipe("a"))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""a.map(_.mkString()).print""")
    assert(generatedCode == expectedCode)
  }

  /******************/
  /* Tests for LOAD */
  /******************/
  it should "contain code for LOAD" in {
    val file = new URI(new java.io.File(".").getCanonicalPath + "/input/file.csv")

    val op = Load(Pipe("a"), file)
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(s"""val a = PigStream[Record]().loadStream(env, "$file", (data: Array[String]) => Record(data))""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD with PigStream" in {
    val file = new URI(new java.io.File(".").getCanonicalPath + "/input/file.csv")
    val op = Load(Pipe("a"), file, None, Some("PigStream"), List("""','"""))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(s"""val a = PigStream[Record]().loadStream(env, "$file", (data: Array[String]) => Record(data), ',')""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD with RDFStream" in {
    val file = new URI(new java.io.File(".").getCanonicalPath + "/file.n3")
    val op = Load(Pipe("a"), file, None, Some("RDFStream"))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(s"""val a = RDFStream[Record]().loadStream(env, "${file}", (data: Array[String]) => Record(data))""")
    assert(generatedCode == expectedCode)
  }

  /*************************/
  /* Tests for SOCKET_READ */
  /*************************/
  it should "contain code for SOCKET_READ" in {
    val op = SocketRead(Pipe("a"), SocketAddress("", "localhost", "9999"), "")
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val a = PigStream().connect(env, "localhost", 9999)""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for SOCKET_READ with PigStream" in {
    val op = SocketRead(Pipe("a"), SocketAddress("", "localhost", "9999"), "", None, Some("PigStream"), List("""','"""))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(s"""val a = PigStream().connect(env, "localhost", 9999, ',')""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for SOCKET_READ with RDFStream" in {
    val op = SocketRead(Pipe("a"), SocketAddress("", "localhost", "9999"), "", None, Some("RDFStream"))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val a = RDFStream().connect(env, "localhost", 9999)""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for SOCKET_READ in ZMQ mode" in {
    val op = SocketRead(Pipe("a"), SocketAddress("tcp://", "localhost", "9999"), "zmq")
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val a = PigStream().zmqSubscribe(env, "tcp://localhost:9999")""")
    assert(generatedCode == expectedCode)
  }

  /**************************/
  /* Tests for SOCKET_WRITE */
  /**************************/
  it should "contain code for SOCKET_WRITE using a Web-Socket" in {
    val op = SocketWrite(Pipe("a"), SocketAddress("", "localhost", "9999"), "")
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""PigStream().bind("localhost", 9999, a)""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for SOCKET_WRITE in ZMQ mode" in {
    val op = SocketWrite(Pipe("a"), SocketAddress("tcp://", "localhost", "9999"), "zmq")
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""PigStream().zmqPublish("tcp://localhost:9999", a)""")
    assert(generatedCode == expectedCode)
  }

  /*******************/
  /* Tests for STORE */
  /*******************/
  it should "contain code for STORE" in {
    val file = new URI(new java.io.File(".").getCanonicalPath + "/input/file.csv")
    val op = Store(Pipe("A"), file)
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(s"""PigStream[Record]().writeStream("${file}", A)""")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for STORE with a known schema" in {
    Schema.init()
    val file = new URI(new java.io.File(".").getCanonicalPath + "/input/file.csv")
    val op = Store(Pipe("A"), file)
    op.schema = Some(Schema(Array(
      Field("f1", Types.IntType),
      Field("f2", BagType(TupleType(Array(Field("f3", Types.DoubleType), Field("f4", Types.DoubleType))))))))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""PigStream[_t$1_Tuple]().writeStream(""""+file+"""", A)""")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for STORE with delimiter" in {
    Schema.init()
    val file = new URI(new java.io.File(".").getCanonicalPath + "/input/file.csv")
    val op = Store(Pipe("A"), file, Some("PigStream"), List(""""#""""))
    op.schema = Some(Schema(Array(
      Field("f1", Types.IntType),
      Field("f2", BagType(TupleType(Array(Field("f3", Types.DoubleType), Field("f4", Types.DoubleType))))))))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""PigStream[_t$1_Tuple]().writeStream(""""+file+"""", A, "#")""")
    generatedCode should matchSnippet(expectedCode)
  }

  /*------------------------------------------------------------------------------------------------- */
  /*                                 Testing of Window Operators                                      */
  /*------------------------------------------------------------------------------------------------- */

  /*********************/
  /* Test for DISTINCT */
  /*********************/
  it should "contain code for DISTINCT" in {
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType), Field("f2", Types.DoubleType)))),"t1")
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
    
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("")
    generatedCode should matchSnippet(expectedCode)

    val generatedHelperCode = cleanString(codeGenerator.emitHelperClass(apply))
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
  
  // TODO
  it should "contain code for simple ORDER BY" in {
    val op = OrderBy(Pipe("B"), Pipe("A"), List(OrderBySpec(PositionalField(0), OrderByDirection.AscendingOrder)))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val B = A.mapWindow(customBOrder _)")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for complex ORDER BY" in {
    val op = OrderBy(Pipe("B"), Pipe("A"), List(OrderBySpec(NamedField("f1"), OrderByDirection.AscendingOrder),
      OrderBySpec(NamedField("f3"), OrderByDirection.AscendingOrder)))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))))
    op.schema = Some(schema)
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val B = A.mapWindow(customBOrder _)")
    assert(generatedCode == expectedCode)

    val generatedHelperCode = cleanString(codeGenerator.emitHelperClass(op))
    val expectedHelperCode = cleanString("""
      |def customBOrder(ts: Iterable[List[Any]], out: Collector[List[Any]]) ={
      |  ts.toList.asInstanceOf[List[List[String]]].sortBy(t => (t(0).asInstanceOf[String],t(2).asInstanceOf[Int])).foreach { x => out.collect(x) }
      |}""".stripMargin)
    assert(generatedHelperCode == expectedHelperCode)
  }

  /********************/
  /* Tests for WINDOW */
  /********************/
  it should "contain code for WINDOW with RANGE size and RANGE slider" in {
    val op = Window(Pipe("b"), Pipe("a"), (5, "SECONDS"), (1, "SECONDS"))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val b = a.windowAll(SlidingTimeWindows.of(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS)))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for WINDOW with RANGE size and ROWS slider" in {
    val op = Window(Pipe("b"), Pipe("a"), (5, "SECONDS"), (10, ""))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val b = a.windowAll(TumblingTimeWindows.of(Time.of(5, TimeUnit.SECONDS))).trigger(CountTrigger.of(10))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for WINDOW with ROWS size and RANGE slider" in {
    val op = Window(Pipe("b"), Pipe("a"), (100, ""), (1, "SECONDS"))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val b = a.windowAll(GlobalWindows.create()).trigger(ContinuousEventTimeTrigger.of(Time.of(1, TimeUnit.SECONDS))).evictor(CountEvictor.of(100))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for WINDOW with ROWS size and ROWS slider" in {
    val op = Window(Pipe("b"), Pipe("a"), (100, ""), (10, ""))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val b = a.windowAll(GlobalWindows.create()).evictor(CountEvictor.of(100)).trigger(CountTrigger.of(10))")
    assert(generatedCode == expectedCode)
  }

  /*------------------------------------------------------------------------------------------------- */
  /*                                Testing of Unifying Operators                                     */
  /*------------------------------------------------------------------------------------------------- */

  /*******************/
  /* Tests for CROSS */
  /*******************/
  it should "contain code for a CROSS operator on two relations" in {
    // a = Cross b, c;
    val file = new java.net.URI("input/file.csv")
    val op = Cross(Pipe("a"), List(Pipe("b"), Pipe("c")), (10, "SECONDS"))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType), Field("f2", Types.DoubleType)))), "t1")
    op.schema = Some(schema)
    val input1 = Pipe("b", Load(Pipe("b"), file, Some(schema), Some("PigStream"), List("\",\"")))
    val input2 = Pipe("c", Load(Pipe("c"), file, Some(schema), Some("PigStream"), List("\",\"")))
    op.inputs = List(input1, input2)
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val a = b.map(t => (1,t)).join(c.map(t => (1,t))).where(t => t._1).equalTo(t => t._1)
      |.window(TumblingTimeWindows.of(Time.SECONDS(10))).apply{ 
      |(t1,t2) => _t$1_Tuple(t1._2._0, t1._2._1, t2._2._0, t2._2._1) 
      |}""".stripMargin.replaceAll("\n", ""))
      
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a CROSS operator on more than two relations" in {
    // a = Cross b, c, d;
    val op = Cross(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")), (10, "SECONDS"))
    val file = new java.net.URI("input/file.csv")
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType), Field("f2", Types.DoubleType)))), "t1")
    op.schema = Some(schema)
    val input1 = Pipe("b", Load(Pipe("b"), file, Some(schema), Some("PigStream"), List("\",\"")))
    val input2 = Pipe("c", Load(Pipe("c"), file, Some(schema), Some("PigStream"), List("\",\"")))
    op.inputs = List(input1, input2)
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val a = b.map(t => (1,t)).join(c.map(t => (1,t))).where(t => t._1).equalTo(t => t._1)
      |.window(TumblingTimeWindows.of(Time.SECONDS(10))).apply{ 
      |  (t1,t2) => _t$1_Tuple(t1._2._0, t1._2._1, t2._2._0, t2._2._1) }
      |.map(t => (1,t)).join(d.map(t => (1,t))).where(t => t._1).equalTo(t => t._1)
      |.window(TumblingTimeWindows.of(Time.SECONDS(10))).apply{ 
      |  (t1,t2) => _t$1_Tuple(t1._2._0, t1._2._1, t2._2._0, t2._2._1) 
      |}""".stripMargin.replaceAll("\n", ""))
    generatedCode should matchSnippet(expectedCode)
  }

  /******************/
  /* Tests for JOIN */
  /******************/
  it should "contain code for a binary JOIN statement with simple expression" in {
    val file = new java.net.URI("input/file.csv")
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c")), List(List(PositionalField(0)), List(PositionalField(0))), (5, "SECONDS"))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))), "t1")
    op.schema = Some(schema)
    val input1 = Pipe("b", Load(Pipe("b"), file, Some(schema), Some("PigStream"), List("\",\"")))
    val input2 = Pipe("c", Load(Pipe("c"), file, Some(schema), Some("PigStream"), List("\",\"")))
    op.inputs = List(input1, input2)
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.join(c).where(t => t._0).equalTo(t => t._0).window(TumblingTimeWindows.of(Time.seconds(5))).apply{ 
        |  (t1,t2) => _t$1_Tuple(t1._0, t1._1, t1._2, t2._0, t2._1, t2._2) 
        |}""".stripMargin.replaceAll("\n", ""))
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a binary JOIN statement with expression lists" in {
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
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.join(c).where(t => Array(t._0,t._1).mkString).equalTo(t => Array(t._1,t._2).mkString)
        |.window(TumblingTimeWindows.of(Time.seconds(5))).apply{ 
        |(t1,t2) => _t1_Tuple(t1._0, t1._1, t1._2, t2._0, t2._1, t2._2) 
        |}""".stripMargin.replaceAll("\n", ""))
        
        
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a multiway JOIN statement" in {
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
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val a = b.join(c).where(t => t._0).equalTo(t => t._0)
      |.window(TumblingTimeWindows.of(Time.seconds(5))).apply{ 
      |  (t1,t2) => _t1_Tuple(t1._0, t1._1, t1._2, t2._0, t2._1, t2._2) }
      |.join(d).where(t => t._0).equalTo(t => t._0)
      |.window(TumblingTimeWindows.of(Time.seconds(5))).apply{ 
      |  (t1,t2) => _t1_Tuple(t1._0, t1._1, t1._2, t2._0, t2._1, t2._2) 
      |}""".stripMargin.replaceAll("\n", ""))
    generatedCode should matchSnippet(expectedCode)
  }

  /*******************/
  /* Tests for UNION */
  /*******************/
  it should "contain code for a UNION operator on two relations" in {
    // a = UNION b, c;
    val op = Union(Pipe("a"), List(Pipe("b"), Pipe("c")))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.union(c)""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a UNION operator on more than two relations" in {
    // a = UNION b, c, d;
    val op = Union(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
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
    val op = Filter(Pipe("a"), Pipe("b"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType)))), "t1")
    op.schema = Some(schema)
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.filter(t => {t._1 < 42})")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for FILTER in window mode" in {
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
    
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("")
    assert(generatedCode == expectedCode)

    val generatedHelperCode = cleanString(codeGenerator.emitHelperClass(apply))
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
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val a = b.map(t => _t$1_Tuple(PigFuncs.toMap("field1",t.get(0),"field2",t.get(1))))""")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a FOREACH statement with function expressions in window mode" in {
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
    
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("")
    generatedCode should matchSnippet(expectedCode)

    val generatedHelperCode = cleanString(codeGenerator.emitHelperClass(apply))
    val expectedHelperCode = cleanString("""
      |def WindowFuncd(wi: Window, ts: Iterable[_t$1_Tuple], out: Collector[_t$1_Tuple]) = { 
      |ts
      |.foreach { t => out.collect(_t1_Tuple(PigFuncs.toMap("field1",t._0,"field2",t._1))) }
      |}""".stripMargin)
    generatedHelperCode should matchSnippet(expectedHelperCode)
  }

  it should "contain code for a FOREACH statement with another function expression" in {
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

    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val a = b.mapWithState(PigFuncs.streamFunc(List(("COUNT", List(1))))).map(t => t._0,t._2)""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for deref operator on maps in FOREACH statement" in {
    // a = FOREACH b GENERATE $0#"k1", $1#"k2";
    val op = Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(GeneratorExpr(RefExpr(DerefMap(PositionalField(0), "\"k1\""))),
      GeneratorExpr(RefExpr(DerefMap(PositionalField(1), "\"k2\""))))))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.map(t => List(t(0).asInstanceOf[Map[String,Any]]("k1"),t(1).asInstanceOf[Map[String,Any]]("k2")))""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for deref operator on tuple in FOREACH statement" in {
    // a = FOREACH b GENERATE $0.$1, $2.$0;
    val op = Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(GeneratorExpr(RefExpr(DerefTuple(PositionalField(0), PositionalField(1)))),
      GeneratorExpr(RefExpr(DerefTuple(PositionalField(2), PositionalField(0)))))))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val a = b.map(t => List(t(0).asInstanceOf[Seq[List[Any]]](0)(1),t(2).asInstanceOf[Seq[List[Any]]](0)(0)))""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  /**********************/
  /* Tests for GROUP BY */
  /**********************/
  it should "contain code for GROUP BY ALL" in {
    val op = Grouping(Pipe("a"), Pipe("b"), GroupingExpression(List()))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val a = b.map(t => List("all", List(t))).groupBy(t => t(0))""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for GROUP BY $0" in {
    val op = Grouping(Pipe("a"), Pipe("b"), GroupingExpression(List(PositionalField(0))))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.map(t => List((t(0)),List(t))).groupBy(t => t(0))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for GROUP BY $0 in window mode" in {
    val op = Grouping(Pipe("a"), Pipe("b"), GroupingExpression(List(PositionalField(0))), true)
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.mapWindow(customaMap _).groupBy(t => t(0))")
    assert(generatedCode == expectedCode)

    val generatedHelperCode = cleanString(codeGenerator.emitHelperClass(op))
    val expectedHelperCode = cleanString("""
      |def customaMap(ts: Iterable[List[Any]], out: Collector[List[Any]]) = {
      |  ts.groupBy(t => t(0)).foreach(t => out.collect(List(t._1,t._2)))
      |}""".stripMargin)
    assert(generatedHelperCode == expectedHelperCode)
  }

  /****************************/
  /* Tests for STREAM THROUGH */
  /****************************/
  it should "contain code for the stream through statement without parameters" in {
    // aa = STREAM bb THROUGH myOp
    val op = StreamOp(Pipe("aa"), Pipe("bb"), "myOp")
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val aa = myOp(env, bb)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for the stream through statement with parameters" in {
    // a = STREAM b THROUGH package.myOp(1, 42.0)
    val op = StreamOp(Pipe("a"), Pipe("b"), "package.myOp", Some(List(Value("1"), Value(42.0))))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val a = package.myOp(env, b,1,42.0)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  /*------------------------------------------------------------------------------------------------- */
  /*                              Testing of Stream Only Operators                                    */
  /*------------------------------------------------------------------------------------------------- */

  /************************/
  /* Tests for SPLIT INTO */
  /************************/
  it should "contain code for SPLIT a INTO b IF f1==2, c IF f2>3" in {
    val op = SplitInto(Pipe("a"), List(SplitBranch(Pipe("b"), Eq(RefExpr(PositionalField(0)), RefExpr(Value(2)))), SplitBranch(Pipe("c"), Gt(RefExpr(PositionalField(1)), RefExpr(Value(3))))))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val b = a.filter(t => {t(0) == 2})
      |val c = a.filter(t => {t(1) > 3})
      |""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  /********************/
  /* Tests for SAMPLE */
  /********************/
  it should "contain code for the SAMPLE operator with a literal value" in {
    val op = Sample(Pipe("a"), Pipe("b"), RefExpr(Value("0.1")))
    val codeGenerator = new FlinkStreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val a = b.filter(t => util.Random.nextDouble <= 0.1)
      |""".stripMargin)
    assert(generatedCode == expectedCode)
  }
}
