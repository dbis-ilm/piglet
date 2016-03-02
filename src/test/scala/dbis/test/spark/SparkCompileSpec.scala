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

import dbis.pig.parser.LanguageFeature
import dbis.pig.parser.PigParser.parseScript
import dbis.pig.backends.BackendManager
import dbis.pig.codegen.spark.BatchCodeGen
import dbis.pig.op._
import dbis.pig.expr._
import dbis.pig.plan.DataflowPlan
import dbis.pig.plan.rewriting.Rewriter._
import dbis.pig.plan.rewriting.Rules
import dbis.pig.schema._
import dbis.pig.udf.UDFTable
import dbis.test.CodeMatchers
import dbis.test.TestTools._
import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}

class SparkCompileSpec extends FlatSpec with BeforeAndAfterAll with Matchers with CodeMatchers {
  
  override def beforeAll()  {
    Rules.registerAllRules()
  }
  
  def cleanString(s: String) : String = s.stripLineEnd.replaceAll("""\s+""", " ").trim
  val backendConf = BackendManager.backend("spark") 
  BackendManager.backend = backendConf 
  val templateFile = backendConf.templateFile

  "The compiler output" should "contain the Spark header & footer" in {
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitImport()
      + codeGenerator.emitHeader1("test")
      + codeGenerator.emitHeader2("test",true)
      + codeGenerator.emitFooter)
    val expectedCode = cleanString("""
        |import org.apache.spark.SparkContext
        |import org.apache.spark.SparkContext._
        |import org.apache.spark.SparkConf
        |import org.apache.spark.rdd._
        |import dbis.pig.backends.{SchemaClass, Record}
        |import dbis.pig.tools._
        |import dbis.pig.backends.spark._
        |
        |object test {
        |    def main(args: Array[String]) {
        |      val conf = new SparkConf().setAppName("test_App")
        |      val sc = new SparkContext(conf)
        |      val perfMon = new PerfMonitor("test_App")
        |      sc.addSparkListener(perfMon)
        |      sc.stop()
        |      
        |    }
        |}
      """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain the Spark header with additional imports" in {
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitImport(Some("import breeze.linalg._"))
      + codeGenerator.emitHeader1("test")
      + codeGenerator.emitHeader2("test",true)
      + codeGenerator.emitFooter)
    val expectedCode = cleanString("""
                                     |import org.apache.spark.SparkContext
                                     |import org.apache.spark.SparkContext._
                                     |import org.apache.spark.SparkConf
                                     |import org.apache.spark.rdd._
                                     |import dbis.pig.backends.{SchemaClass, Record}
                                     |import dbis.pig.tools._
                                     |import dbis.pig.backends.spark._
                                     |import breeze.linalg._
                                     |
                                     |object test {
                                     |    def main(args: Array[String]) {
                                     |      val conf = new SparkConf().setAppName("test_App")
                                     |      val sc = new SparkContext(conf)
                                     |      val perfMon = new PerfMonitor("test_App")
                                     |      sc.addSparkListener(perfMon)
                                     |      sc.stop()
                                     |
                                     |    }
                                     |}
                                   """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD" in {
    
    val file = new java.io.File(".").getCanonicalPath + "/input/file.csv"
    
    val op = Load(Pipe("a"), file)
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(s"""
         |val a = PigStorage[Record]().load(sc, "${file}", (data: Array[String]) => Record(data))""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD with renamed pipe" in {
    val file = new java.io.File(".").getCanonicalPath + "/input/file.csv"
    val op = Load(Pipe("a"), file)
    op.outputs = List(Pipe("b"))
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(s"""
         |val b = PigStorage[Record]().load(sc, "${file}", (data: Array[String]) => Record(data))""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD with PigStorage" in {
    
    val file = new java.io.File(".").getCanonicalPath + "/input/file.csv"
    
    val op = Load(Pipe("a"), file, None, Some("PigStorage"), List("""",""""))
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(s"""
         |val a = PigStorage[Record]().load(sc, "${file}", (data: Array[String]) => Record(data), ",")""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD with schema" in {
    val ops = parseScript(
      """
        |A = LOAD 'file.csv' USING PigStorage(',') AS (f1: int, f2: chararray, f3: double);
      """.stripMargin
    )
    val plan = new DataflowPlan(ops)
    val codeGenerator = new BatchCodeGen(templateFile)
    val op = plan.findOperatorForAlias("A").get
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
         |val A = PigStorage[_t$1_Tuple]().load(sc, "file.csv",
         |(data: Array[String]) => _t$1_Tuple(data(0).toInt, data(1).toString, data(2).toDouble), ",")""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
    //    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD with RDFFileStorage" in {
    Schema.init()

    val file = new java.io.File(".").getCanonicalPath + "/file.n3"
    
    val op = Load(Pipe("a"), file, None, Some("RDFFileStorage"))
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString( s"""
         |val a = RDFFileStorage[Record]().load(sc, "${file}", (data: Array[String]) => Record(data))""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for FILTER" in {
    val op = Filter(Pipe("aa"), Pipe("bb"), Lt(RefExpr(PositionalField(1)), RefExpr(Value(42))))
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = bb.filter(t => {t.get(1) < 42})")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a complex FILTER" in {
    val ops = parseScript(
      """b = LOAD 'file' AS (x: double, y:double, z1:int, z2: int);
        |c = FILTER b BY x > 0 AND (y < 0 OR (NOT z1 == z2));""".stripMargin)
    val plan = new DataflowPlan(ops)
    val op = ops(1)
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val c = b.filter(t => {t._0 > 0 && (t._1 < 0 || (!(t._2 == t._3)))})")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a filter with a function expression" in {
    Schema.init()
    val op = Filter(Pipe("a"), Pipe("b"), Gt(
        Func("aFunc", List(RefExpr(PositionalField(0)), RefExpr(PositionalField(1)))),
        RefExpr(Value("0"))))
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val a = b.filter(t => {aFunc(t.get(0),t.get(1)) > 0})")
    assert(generatedCode == expectedCode)
  }
  
  it should "contain code for a filter with a function expression and boolean" in {
    Schema.init()
    val op =  Filter(Pipe("a"),Pipe("b"),And(
            Eq(Func("aFunc",List(RefExpr(NamedField("x")), RefExpr(NamedField("y")))),RefExpr(Value(true))),
            Geq(Func("cFunc",List(RefExpr(NamedField("x")), RefExpr(NamedField("y")))),RefExpr(NamedField("x")))),false)
    op.schema = Some(Schema(Array(Field("x", Types.IntType),
                                                        Field("y", Types.DoubleType))))
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val a = b.filter(t => {aFunc(t._0,t._1) == true && cFunc(t._0,t._1) >= t._0})
      |""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code a filter with an expression on a string literal" in {
    val ops = parseScript("""b = LOAD 'file'; a = FILTER b BY $0 == 'aString';""")
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("a").get

    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))

    val expectedCode = cleanString("""
                                     |val a = b.filter(t => {t.get(0) == "aString"})
                                     |""".stripMargin)
    assert(generatedCode == expectedCode)
  }


  it should "contain code for DUMP" in {
    val op = Dump(Pipe("a"))
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""a.collect.map(t => println(t.mkString()))""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for STORE" in {
    Schema.init()
    val file = new java.io.File(".").getCanonicalPath + "/input/file.csv"
    
    val op = Store(Pipe("A"), file)
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(
      s"""
         |val A_helper = A.coalesce(1, true)
         |PigStorage[Record]().write("$file", A_helper)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for STORE with a known schema" in {
    Schema.init()
    val op = Store(Pipe("A"), "input/file.csv")
    op.schema = Some(Schema(Array(
      Field("f1", Types.IntType),
      Field("f2", BagType(TupleType(Array(Field("f3", Types.DoubleType), Field("f4", Types.DoubleType))))))))

    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(
      """
         |val A_helper = A.coalesce(1, true)
         |PigStorage[Record]().write("input/file.csv", A_helper)""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }
  
  it should "contain code for STORE with delimiter" in {
    Schema.init()
    val op = Store(Pipe("A"), "input/file.csv", Some("PigStorage"), List(""""#""""))
    op.schema = Some(Schema(Array(
      Field("f1", Types.IntType),
      Field("f2", BagType(TupleType(Array(Field("f3", Types.DoubleType), Field("f4", Types.DoubleType))))))))

    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(
      """
         |val A_helper = A.coalesce(1, true)
         |PigStorage[Record]().write("input/file.csv", A_helper, "#")""".stripMargin)
   generatedCode should matchSnippet(expectedCode)
  }
  
  it should "contain code for STORE with using clause" in {
    val file = new java.io.File(".").getCanonicalPath + "/input/file.csv"
    
    val op = Store(Pipe("A"), file, Some("BinStorage"))
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    
    val expectedCode = cleanString(
      s"""
         |val A_helper = A.coalesce(1, true)
         |BinStorage[Record]().write("$file", A_helper)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  
  it should "contain code for GROUP BY ALL" in {
    val ops = parseScript(
      """
        |bb = LOAD 'file.csv' USING PigStorage(',') AS (f1: int, f2: chararray, f3: double);
        |aa = GROUP bb ALL;
      """.stripMargin
    )
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("aa").get
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""val aa = bb.coalesce(1).glom.map(t => _t2_Tuple("all", t))""")
    assert(generatedCode == expectedCode)

    val schemaCode = cleanString(codeGenerator.emitSchemaClass(op.schema.get))
    val expectedSchemaCode =
      cleanString("""
         |case class _t$1_Tuple (_0: String, _1: Iterable[_t$2_Tuple]) extends java.io.Serializable with SchemaClass {
         |  override def mkString(_c: String = ",") = _0 + _c + "{" + _1.mkString(",") + "}"
         |}
         |implicit def convert_t2_Tuple(t: (String, Iterable[_t1_Tuple])): _t2_Tuple = _t2_Tuple(t._1, t._2)
       """.stripMargin)
    schemaCode should matchSnippet(expectedSchemaCode)
  }

  it should "contain code for GROUP BY $0" in {
    val ops = parseScript(
      """
        |bb = LOAD 'file.csv' USING PigStorage(',') AS (f1: int, f2: chararray, f3: double);
        |aa = GROUP bb BY f1;
      """.stripMargin
    )
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("aa").get
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = bb.groupBy(t => {t._0}).map{case (k,v) => _t$1_Tuple(k,v)}")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for GROUP BY with multiple keys" in {
    val ops = parseScript(
      """
        |bb = LOAD 'file.csv' USING PigStorage(',') AS (f1: int, f2: chararray, f3: double);
        |aa = GROUP bb BY ($0, $1);
      """.stripMargin
    )
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("aa").get
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(
      """val aa = bb.groupBy(t => {(t._0,t._1)}).map{case (k,v) => _t$1_Tuple(_t$2_Tuple(k._1, k._2),v)}""")
    // val schemaClassCode = cleanString(codeGenerator.emitSchemaClass(op.schema.get))
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for DISTINCT" in {
    val op = Distinct(Pipe("aa"), Pipe("bb"))
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = bb.distinct")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for Limit" in {
    val op = Limit(Pipe("aa"), Pipe("bb"), 10)
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = sc.parallelize(bb.take(10))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a binary join statement with simple expression" in {
    Schema.init()
    val op = Join(Pipe("aa"), List(Pipe("bb"), Pipe("cc")), List(List(PositionalField(0)), List(PositionalField(0))))
    val schema = Schema(Array(Field("f1", Types.CharArrayType),
                                                              Field("f2", Types.DoubleType),
                                                              Field("f3", Types.IntType)))
    val input1 = Pipe("bb",Load(Pipe("bb"), "input/file.csv", Some(schema), Some("PigStorage"), List("\",\"")))
    val input2 = Pipe("cc",Load(Pipe("cc"), "input/file.csv", Some(schema), Some("PigStorage"), List("\",\"")))
    op.inputs = List(input1,input2)
    op.constructSchema

    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val bb_kv = bb.map(t => (t._0,t))
      |val cc_kv = cc.map(t => (t._0,t))
      |val aa = bb_kv.join(cc_kv).map{case (k,(v,w)) => _t$1_Tuple(v._0, v._1, v._2, w._0, w._1, w._2)}""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a binary join statement with expression lists" in {
    Schema.init()
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c")), List(List(PositionalField(0), PositionalField(1)),
      List(PositionalField(1), PositionalField(2))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
                                                              Field("f2", Types.DoubleType),
                                                              Field("f3", Types.IntType)))))
    val input1 = Pipe("b",Load(Pipe("b"), "input/file.csv", Some(schema), Some("PigStorage"), List("\",\"")))
    val input2 = Pipe("c",Load(Pipe("c"), "input/file.csv", Some(schema), Some("PigStorage"), List("\",\"")))
    op.inputs = List(input1,input2)
    op.constructSchema

    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val b_kv = b.map(t => (Array(t._0,t._1).mkString,t))
      |val c_kv = c.map(t => (Array(t._1,t._2).mkString,t))
      |val a = b_kv.join(c_kv).map{case (k,(v,w)) => _t$1_Tuple(v._0, v._1, v._2, w._0, w._1, w._2)}""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a multiway join statement" in {
    Schema.init()
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")), List(List(PositionalField(0)),
      List(PositionalField(0)), List(PositionalField(0))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
                                                              Field("f2", Types.DoubleType),
                                                              Field("f3", Types.IntType)))))
    val input1 = Pipe("b",Load(Pipe("b"), "input/file.csv", Some(schema), Some("PigStorage"), List("\",\"")))
    val input2 = Pipe("c",Load(Pipe("c"), "input/file.csv", Some(schema), Some("PigStorage"), List("\",\"")))
    val input3 = Pipe("d",Load(Pipe("d"), "input/file.csv", Some(schema), Some("PigStorage"), List("\",\"")))
    op.inputs = List(input1,input2,input3)
    op.constructSchema
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val b_kv = b.map(t => (t._0,t))
      |val c_kv = c.map(t => (t._0,t))
      |val d_kv = d.map(t => (t._0,t))
      |val a = b_kv.join(c_kv).join(d_kv).map{case (k,((v1,v2),v3)) => _t$1_Tuple(v1._0, v1._1, v1._2, v2._0, v2._1, v2._2, v3._0, v3._1, v3._2)}""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for multiple joins" in {
    val ops = parseScript(
      """a = load 'file' as (a: chararray);
        |b = load 'file' as (a: chararray);
        |c = load 'file' as (a: chararray);
        |j1 = join a by $0, b by $0;
        |j2 = join a by $0, c by $0;
        |j = join j1 by $0, j2 by $0;
        |""".stripMargin)
    val plan = new DataflowPlan(ops)
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode1 = cleanString(codeGenerator.emitNode(plan.findOperatorForAlias("j1").get))
    val generatedCode2 = cleanString(codeGenerator.emitNode(plan.findOperatorForAlias("j2").get))
    val generatedCode3 = cleanString(codeGenerator.emitNode(plan.findOperatorForAlias("j").get))

    val finalJoinOp = plan.findOperatorForAlias("j").get
    // TODO: Schema classes!!!!
    val schemaClassCode = cleanString(codeGenerator.emitSchemaClass(finalJoinOp.schema.get))

    val expectedCode1 = cleanString(
      """val a_kv = a.map(t => (t._0,t))
        |val b_kv = b.map(t => (t._0,t))
        |val j1 = a_kv.join(b_kv).map{case (k,(v,w)) => _t$1_Tuple(v._0, w._0)}""".stripMargin)
    generatedCode1 should matchSnippet(expectedCode1)

    val expectedCode2 = cleanString(
      """val c_kv = c.map(t => (t._0,t))
        |val j2 = a_kv.join(c_kv).map{case (k,(v,w)) => _t$1_Tuple(v._0, w._0)}""".stripMargin)
    generatedCode2 should matchSnippet(expectedCode2)

    val expectedCode3 = cleanString(
      """val j1_kv = j1.map(t => (t._0,t))
        |val j2_kv = j2.map(t => (t._0,t))
        |val j = j1_kv.join(j2_kv).map{case (k,(v,w)) => _t$1_Tuple(v._0, v._1, w._0, w._1)}""".stripMargin)
    generatedCode3 should matchSnippet(expectedCode3)

  }

  it should "contain code a foreach statement with function expressions" in {
    Schema.init()
    // a = FOREACH b GENERATE TOMAP("field1", $0, "field2", $1);
    val op = Foreach(Pipe("aa"), Pipe("bb"), GeneratorList(List(
      GeneratorExpr(Func("TOMAP", List(
        RefExpr(Value("\"field1\"")),
        RefExpr(PositionalField(0)),
        RefExpr(Value("\"field2\"")),
        RefExpr(PositionalField(1)))))
    )))
    op.constructSchema
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = bb.map(t => _t$1_Tuple(PigFuncs.toMap(\"field1\",t.get(0),\"field2\",t.get(1))))")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a foreach statement with another function expression" in {
    Schema.init()
    // a = FOREACH b GENERATE $0, COUNT($1) AS CNT;
    val op = Foreach(Pipe("aa"), Pipe("bb"), GeneratorList(List(
        GeneratorExpr(RefExpr(PositionalField(0))),
        GeneratorExpr(Func("COUNT", List(RefExpr(PositionalField(1)))), Some(Field("CNT", Types.LongType)))
      )))
    op.constructSchema
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = bb.map(t => _t$1_Tuple(t.get(0), PigFuncs.count(t.get(1))))")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a foreach statement with a UDF expression" in {
    // aa = FOREACH bb GENERATE $0, distance($1, $2, 1.0, 2.0) AS dist;
    val ops = parseScript(
      """
      |bb = LOAD 'file' AS (f1: int, f2: int, f3: int);
      |aa = FOREACH bb GENERATE $0, Distances.spatialDistance($1, $2, 1.0, 2.0) AS dist;
    """.stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("aa").get
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = bb.map(t => _t$1_Tuple(t._0, Distances.spatialDistance(t._1,t._2,1.0,2.0)))")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a foreach statement with a UDF alias expression" in {
    // aa = FOREACH bb GENERATE $0, distance($1, $2, 1.0, 2.0) AS dist;
    val ops = parseScript(
      """bb = LOAD 'data.csv' AS (t1: int, t2: int, t3: int, t4: int);
        |DEFINE distance Distances.spatialDistance();
        |aa = FOREACH bb GENERATE $0, distance($1, $2, 1.0, 2.0) AS dist;
        |""".stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("aa").get
    // op.constructSchema
    val codeGenerator = new BatchCodeGen(templateFile)
    // this is just a hack for this test: normally, the udfAliases map is set in compile
    codeGenerator.udfAliases = Some(plan.udfAliases.toMap)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = bb.map(t => _t$1_Tuple(t._0, Distances.spatialDistance(t._1,t._2,1.0,2.0)))")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for deref operator on maps in foreach statement" in {
    val ops = parseScript(
    """
      |in = LOAD 'file' AS (s1: chararray, s2: chararray);
      |b = FOREACH in GENERATE ["k1", s1] as map1, ["k2", s2] as map2;
      |a = FOREACH b GENERATE $0#"k1", $1#"k2";
    """.stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("a").get

    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val a = b.map(t => _t$1_Tuple(t._0("k1"), t._1("k2")))""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for deref operator on tuple in foreach statement" in {
    val ops = parseScript(
      """
        |in = LOAD 'file' AS (s1: int, s2: int, s3: int);
        |b = FOREACH in GENERATE ("k1", s1) as t1, ("k2", s2) as t2, ("k3", s3) as t3;
        |a = FOREACH b GENERATE $0.$1, $2.$0;
      """.stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("a").get

    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.map(t => _t$1_Tuple(t._0._1, t._2._0))""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a nested foreach statement" in {
    val ops = parseScript(
      """daily = load 'data.csv' as (exchange, symbol);
        |grpd  = group daily by exchange;
        |uniqcnt  = foreach grpd {
        |           sym      = daily.symbol;
        |           uniq_sym = distinct sym;
        |           generate group, COUNT(uniq_sym);
        |};""".stripMargin)
    val plan = new DataflowPlan(ops)
    val foreachOp = plan.findOperatorForAlias("uniqcnt").get
    println("schema = " + foreachOp.schema)
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(foreachOp))
    val expectedCode = cleanString(
      """val uniqcnt = grpd.map(t => {
        |val sym = t._1.map(l => l._1).toList
        |val uniq_sym = sym.distinct
        |_t$1_Tuple(t._0, PigFuncs.count(uniq_sym))})""".stripMargin)

    generatedCode should matchSnippet(expectedCode)
    val schemaClassCode = cleanString(codeGenerator.emitSchemaClass(foreachOp.schema.get))
  }

  it should "contain code for a foreach statement with constructors for tuple, bag, and map" in {
    val ops = parseScript(
      """data = load 'file' as (f1: int, f2: int, name:chararray);
        |out = foreach data generate (f1, f2), {f1, f2}, [name, f1];""".stripMargin)
    val plan = new DataflowPlan(ops)
    val codeGenerator = new BatchCodeGen(templateFile)
    val op = plan.findOperatorForAlias("out").get
    val schemaClassCode = cleanString(codeGenerator.emitSchemaClass(op.schema.get))

    val generatedCode = cleanString(codeGenerator.emitNode(op))
    println("schema class = " + schemaClassCode)

    val expectedCode = cleanString(
      """val out = data.map(t => _t$1_Tuple(_t$2_Tuple(t._0,t._1), List(_t$3_Tuple(t._0),_t$3_Tuple(t._1)),
        |Map[String,Int](t._2 -> t._0)))""".stripMargin)

    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a union operator on two relations" in {
    // a = UNION b, c;
    val op = Union(Pipe("aa"), List(Pipe("bb"), Pipe("cc")))
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val aa = bb.union(cc)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a union operator on more than two relations" in {
    // a = UNION b, c, d;
    val op = Union(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")))
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.union(c).union(d)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for the sample operator with a literal value" in {
    // aa = SAMPLE bb 0.01;
    val op = Sample(Pipe("aa"), Pipe("bb"), RefExpr(Value(0.01)))
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val aa = bb.sample(false, 0.01)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for the sample operator with an expression" in {
    // a = SAMPLE b 100 / $3
    val op = Sample(Pipe("a"), Pipe("b"), Div(RefExpr(Value(100)), RefExpr(PositionalField(3))))
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.sample(false, 100 / t.get(3))""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for the stream through statement without parameters" in {
    val ops = parseScript(
      """data = load 'data.csv' as (f1: int, f2: int);
        |res = STREAM data THROUGH myOp();""".stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("res").get

    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(
      """val data_helper = data.map(t => List(t._0, t._1))
        |val res = myOp(sc, data_helper).map(t => _t$1_Tuple(t(0), t(1)))
        |""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for the stream through statement with parameters" in {
    val ops = parseScript(
      """data = load 'data.csv' as (f1: int, f2: int);
        |res = STREAM data THROUGH package.myOp(1, 42.0);""".stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("res").get

    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(
      """val data_helper = data.map(t => List(t._0, t._1))
        |val res = package.myOp(sc, data_helper,1,42.0).map(t => _t$1_Tuple(t(0), t(1)))
        |""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for simple ORDER BY" in {
    // aa = ORDER bb BY $0
    val op = OrderBy(Pipe("aa"), Pipe("bb"), List(OrderBySpec(PositionalField(0), OrderByDirection.AscendingOrder)))
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val aa = bb.keyBy(t => t.get(0)).sortByKey(true).map{case (k,v) => v}""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for complex ORDER BY" in {
    Schema.init()
    // a = ORDER b BY f1, f3
    val op = OrderBy(Pipe("a"), Pipe("b"), List(OrderBySpec(NamedField("f1"), OrderByDirection.AscendingOrder),
                                    OrderBySpec(NamedField("f3"), OrderByDirection.AscendingOrder)))
    val schema = Schema(Array(Field("f1", Types.CharArrayType),
                                                              Field("f2", Types.DoubleType),
                                                              Field("f3", Types.IntType)))

    op.schema = Some(schema)
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.keyBy(t => custKey_a_b(t._0,t._2)).sortByKey(true).map{case (k,v) => v}""".stripMargin)
    assert(generatedCode == expectedCode)

    val generatedHelperCode = cleanString(codeGenerator.emitHelperClass(op))
    val expectedHelperCode = cleanString("""
        |case class custKey_a_b(c1: String, c2: Int) extends Ordered[custKey_a_b] {
        |  def compare(that: custKey_a_b) = { if (this.c1 == that.c1) {
        |                                    this.c2 compare that.c2
        |                                 }
        |                                 else
        |                                   this.c1 compare that.c1 }
        |}""".stripMargin)
    assert(generatedHelperCode == expectedHelperCode)
  }

  it should "contain code for flattening a tuple in FOREACH" in {
    Schema.init()
    val ops = parseScript("b = load 'file'; a = foreach b generate $0, flatten($1);")
    val schema = Schema(Array(
      Field("f1", Types.CharArrayType),
      Field("f2", TupleType(Array(
        Field("f3", Types.IntType),
        Field("f4", Types.CharArrayType))))))
    ops.head.schema = Some(schema)
    val plan = new DataflowPlan(ops)
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(plan.findOperatorForAlias("a").get))
    val expectedCode = cleanString("""
        |val a = b.map(t => _t$1_Tuple(t._0, t._1._0, t._1._1))""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for flattening a bag function in FOREACH" in {
    val ops = parseScript("""
                         |b = load 'file';
                         |a = foreach b generate flatten(tokenize($0));""".stripMargin)
    val schema = Schema(Array(Field("f1", Types.CharArrayType)))
    ops.head.schema = Some(schema)
    val plan = new DataflowPlan(ops)
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(plan.findOperatorForAlias("a").get))
    val expectedCode = cleanString("""
        |val a = b.flatMap(t => PigFuncs.tokenize(t._0).map(_t$1_Tuple(_)).map(t => _t$2_Tuple(t._0)))""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for flattening a bag in FOREACH" in {
    val ops = parseScript("b = load 'file'; a = foreach b generate $0, flatten($1);")
    val schema = Schema(Array(Field("f1", Types.CharArrayType),
      Field("f2", BagType(TupleType(Array(Field("ff1", Types.IntType)))))))
    ops.head.schema = Some(schema)
    val plan = new DataflowPlan(ops)
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(plan.findOperatorForAlias("a").get))
    val expectedCode = cleanString("""
        |val a = b.flatMap(t => t._1.map(s => _t$1_Tuple(t._0, s)))""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a simple accumulate statement" in {
    val ops = parseScript("b = load 'file'; a = ACCUMULATE b GENERATE COUNT($0), AVG($1), SUM($2);")
    val schema = Schema(Array(Field("t1", Types.IntType),
                              Field("t2", Types.IntType),
                              Field("t3", Types.IntType)))
    ops.head.schema = Some(schema)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("a").get
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(
      """
        |def aggr_a_seq(acc: _t$2_HelperTuple, v: _t$2_HelperTuple): _t$2_HelperTuple =
        |                _t$2_HelperTuple(v._t, PigFuncs.incrCOUNT(acc._0, v._0), PigFuncs.incrSUM(acc._1sum, v._1sum),
        |                             PigFuncs.incrCOUNT(acc._1cnt, v._1cnt), PigFuncs.incrSUM(acc._2, v._2))
        |def aggr_a_comp(acc: _t$2_HelperTuple, v: _t$2_HelperTuple): _t$2_HelperTuple =
        |                _t$2_HelperTuple(v._t, PigFuncs.incrSUM(acc._0, v._0), PigFuncs.incrSUM(acc._1sum, v._1sum),
        |                             PigFuncs.incrSUM(acc._1cnt, v._1cnt), PigFuncs.incrSUM(acc._2, v._2))
        |val a_fold = b.map(t => _t$2_HelperTuple(t, 0, t._1, t._1, t._2))
        |               .aggregate(_t$2_HelperTuple())(aggr_a_seq, aggr_a_comp)
        |val a = sc.parallelize(Array(_t$2_Tuple(a_fold._0, a_fold._1sum.toDouble / a_fold._1cnt.toDouble, a_fold._2)))
        |""".stripMargin)

    val generatedHelperClass = cleanString(codeGenerator.emitHelperClass(op))
    val expectedHelperClass = cleanString(
    """case class _t$2_HelperTuple (_t: _t$1_Tuple = null, _0: Long = 0, _1sum: Long = 0, _1cnt: Long = 0, _2: Int = 0)
      |extends java.io.Serializable with SchemaClass { override def mkString(_c: String = ",") = "" }
      |""".stripMargin)
    generatedHelperClass should matchSnippet(expectedHelperClass)
    generatedCode should matchSnippet(expectedCode)

  }

  it should "not contain code for EMPTY operators" in {
    val op = Empty(Pipe("_"))
    val codeGenerator = new BatchCodeGen(templateFile)

    assert(codeGenerator.emitNode(op) == "")
  }

  it should "contain embedded code" in {
    val ops = parseScript(
      """
        |<% def someFunc(s: String): String = {
        | s
        |}
        |%>
        |A = LOAD 'file.csv';
      """.stripMargin)
    val plan = new DataflowPlan(ops)
    val codeGenerator = new BatchCodeGen(templateFile)
    assert(cleanString(codeGenerator.emitHeader1("test", plan.code)) ==
      cleanString("""
        |object test {
        |def someFunc(s: String): String = {
        | s
        |}
      """.stripMargin))
    val udf = UDFTable.findUDF("someFunc", Types.AnyType)
    udf shouldBe defined
  }

  it should "contain code for macros" in {
    val ops = parseScript(
    """
      |DEFINE my_macro(in_alias, p) RETURNS out_alias {
      |$out_alias = FOREACH $in_alias GENERATE $0 + $p;
      |};
      |
      |in = LOAD 'file' AS (i: double);
      |out = my_macro(in, 42);
      |DUMP out;
    """.stripMargin
    )
    val plan = new DataflowPlan(ops)
    val rewrittenPlan = processPlan(plan)
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(rewrittenPlan.findOperatorForAlias("out").get))
    val expectedCode = cleanString(
      """
        |val out = in.map(t => _t$1_Tuple(t._0 + 42))
        |""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for multiple macros" in {
    val ops = parseScript(
      """
        |DEFINE my_macro(in_alias, p) RETURNS out_alias {
        |$out_alias = FOREACH $in_alias GENERATE $0 + $p, $1;
        |};
        |
        |DEFINE my_macro2(in_alias, p) RETURNS out_alias {
        |$out_alias = FOREACH $in_alias GENERATE $0, $1 - $p;
        |};
        |
        |in = LOAD 'file' AS (c1: int, c2: int);
        |out = my_macro(in, 42);
        |out2 = my_macro2(out, 5);
        |DUMP out;
        |DUMP out2;
      """.stripMargin
    )
    val plan = new DataflowPlan(ops)
    val rewrittenPlan = processPlan(plan)
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode1 = cleanString(codeGenerator.emitNode(rewrittenPlan.findOperatorForAlias("out").get))
    val expectedCode1 = cleanString(
      """
        |val out = in.map(t => _t$1_Tuple(t._0 + 42, t._1))
        |""".stripMargin)
    generatedCode1 should matchSnippet(expectedCode1)
    val generatedCode2 = cleanString(codeGenerator.emitNode(rewrittenPlan.findOperatorForAlias("out2").get))
    val expectedCode2 = cleanString(
      """
        |val out2 = out.map(t => _t$1_Tuple(t._0, t._1 - 5))
        |""".stripMargin)
    generatedCode2 should matchSnippet(expectedCode2)
  }

  it should "contain code for invoking a macro multiple times" in {
    val ops = parseScript(
      """
        |DEFINE my_macro(in_alias, p) RETURNS out_alias {
        |$out_alias = FOREACH $in_alias GENERATE $0 + $p;
        |};
        |
        |in = LOAD 'file' AS (c1: int, c2: int);
        |out = my_macro(in, 42);
        |out2 = my_macro(out, 43);
        |DUMP out2;
      """.stripMargin
    )
    val plan = new DataflowPlan(ops)
    val rewrittenPlan = processPlan(plan)
    val codeGenerator = new BatchCodeGen(templateFile)
    val generatedCode1 = cleanString(codeGenerator.emitNode(rewrittenPlan.findOperatorForAlias("out").get))
    val expectedCode1 = cleanString(
      """
        |val out = in.map(t => _t3_Tuple(t._0 + 42))
        |""".stripMargin)
    assert(generatedCode1 == expectedCode1)
    val generatedCode2 = cleanString(codeGenerator.emitNode(rewrittenPlan.findOperatorForAlias("out2").get))
    val expectedCode2 = cleanString(
      """
        |val out2 = out.map(t => _t3_Tuple(t._0 + 43))
        |""".stripMargin)
    assert(generatedCode2 == expectedCode2)
  }

  it should "contain code for schema classes" in {
    val ops = parseScript(
    """
      |A = LOAD 'file' AS (f1: int, f2: chararray, f3: double);
      |B = FILTER A BY f1 > 0;
      |C = FOREACH B GENERATE f1, f2, f3 + 5, $2 + 44 AS f4:int;
      |DUMP C;
    """.stripMargin
    )
    val plan = new DataflowPlan(ops)
    val rewrittenPlan = processPlan(plan)
    val codeGenerator = new BatchCodeGen(templateFile)

    var code: String = ""
    for (schema <- Schema.schemaList) {
      code = code + codeGenerator.emitSchemaClass(schema)
    }

    val generatedCode = cleanString(code)
    val expectedCode = cleanString(
    """
      |case class _t$2_Tuple (_0: Int, _1: String, _2: Double, _3: Int) extends java.io.Serializable with SchemaClass {
      |  override def mkString(_c: String = ",") = _0 + _c + _1 + _c + _2 + _c + _3
      |}
      |implicit def convert_t2_Tuple(t: (Int, String, Double, Int)): _t2_Tuple = _t2_Tuple(t._1, t._2, t._3, t._4)
      |case class _t$1_Tuple (_0: Int, _1: String, _2: Double) extends java.io.Serializable with SchemaClass {
      |  override def mkString(_c: String = ",") = _0 + _c + _1 + _c + _2
      |}
      |implicit def convert_t1_Tuple(t: (Int, String, Double)): _t1_Tuple = _t1_Tuple(t._1, t._2, t._3)
      |""".stripMargin
    )
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for nested schema classes" in {
    val ops = parseScript(
      """
        |daily = load 'file' using PigStorage(',') as (exchange: chararray, symbol: chararray);
        |grpd  = group daily by exchange;
        |DUMP grpd;
      """.stripMargin
    )
    val plan = new DataflowPlan(ops)
    val rewrittenPlan = processPlan(plan)
    val codeGenerator = new BatchCodeGen(templateFile)

    var code: String = ""
    for (schema <- Schema.schemaList) {
      code = code + codeGenerator.emitSchemaClass(schema)
    }

    val generatedCode = cleanString(code)
    val expectedCode = cleanString(
      """
        |case class _t$1_Tuple (_0: String, _1: String) extends java.io.Serializable with SchemaClass {
        |  override def mkString(_c: String = ",") = _0 + _c + _1
        |}
        |implicit def convert_t1_Tuple(t: (String, String)): _t1_Tuple = _t1_Tuple(t._1, t._2)
        |case class _t$2_Tuple (_0: String, _1: Iterable[_t$1_Tuple]) extends java.io.Serializable with SchemaClass {
        |  override def mkString(_c: String = ",") = _0 + _c + "{" + _1.mkString(",") + "}"
        |}
        |implicit def convert_t2_Tuple(t: (String, Iterable[_t1_Tuple])): _t2_Tuple = _t2_Tuple(t._1, t._2)
        |""".stripMargin
    )
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code to handle LOAD with PigStorage but without an explicit schema" in {
    val ops = parseScript(
    """
      |in = load 'file' using PigStorage(':');
      |out = filter in by $1 == "root";
      |dump out;
    """.stripMargin)
    val plan = new DataflowPlan(ops)
    val rewrittenPlan = processPlan(plan)
    val codeGenerator = new BatchCodeGen(templateFile)
    val op = rewrittenPlan.findOperatorForAlias("out").get
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(
    """
      |val out = in.filter(t => {t.get(1) == "root"})
    """.stripMargin)
    assert (generatedCode == expectedCode)
  }

  it should "contain correct code for a function call with bytearray parameters" in {
    val ops = parseScript(
    """
      |in = load 'file' as (x, y);
      |in2 = foreach in generate x, y;
      |out = foreach in2 generate tokenize(x);
      |dump out;
    """.stripMargin)
    val plan = new DataflowPlan(ops)
    val rewrittenPlan = processPlan(plan)
    val codeGenerator = new BatchCodeGen(templateFile)
    val op = rewrittenPlan.findOperatorForAlias("out").get
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(
      """
        |val out = in2.map(t => _t$2_Tuple(PigFuncs.tokenize(t._0.asInstanceOf[String]).map(_t$1_Tuple(_))))
      """.stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "generate code for GROUP BY with group name" in {
    val plan = new DataflowPlan(parseScript(
      """
        |A = LOAD 'file' AS (name, value: double);
        |B = GROUP A BY name;
        |C = FOREACH B GENERATE A.name, AVG(A.value);
        |DUMP C;
      """.stripMargin))
    val rewrittenPlan = processPlan(plan)
    val codeGenerator = new BatchCodeGen(templateFile)
    val op = rewrittenPlan.findOperatorForAlias("C").get
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(
      """
        |val C = B.map(t => _t$1_Tuple(t._0, PigFuncs.average(t._1.map(e => e._1))))
      """.stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "generate code for matrix construction" in {
    val plan = new DataflowPlan(parseScript(
      """
        |A = LOAD 'file' AS (v11: double, v12: double, v21: double, v22: double, v31: double, v32: double);
        |B = FOREACH A GENERATE ddmatrix(2, 3, {v11, v12, v21, v22, v31, v32});
        |DUMP B;
      """.stripMargin))
    val rewrittenPlan = processPlan(plan)
    val codeGenerator = new BatchCodeGen(templateFile)
    val op = rewrittenPlan.findOperatorForAlias("B").get
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(
      """
        |val B = A.map(t => _t$1_Tuple(new DenseMatrix[Double](2, 3, List(_t$2_Tuple(t._0),_t$2_Tuple(t._1),_t$2_Tuple(t._2),_t$2_Tuple(t._3),_t$2_Tuple(t._4),_t$2_Tuple(t._5)).map(v => v._0).toArray)))
        |""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "generate code for constructing a matrix from a bag" in {
    val plan = new DataflowPlan(parseScript(
      """
        |A = LOAD 'file' AS (v11: double, v12: double, v21: double, v22: double, v31: double, v32: double);
        |B = FOREACH A GENERATE {v11, v12, v21, v22, v31, v32} as myBag;
        |C = FOREACH B GENERATE ddmatrix(2, 3, myBag);
        |DUMP C;
      """.stripMargin))
    val rewrittenPlan = processPlan(plan)
    val codeGenerator = new BatchCodeGen(templateFile)
    val op = rewrittenPlan.findOperatorForAlias("C").get
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(
      """
        |val C = B.map(t => _t$1_Tuple(new DenseMatrix[Double](2, 3, t._0.map(v => v._0).toArray)))
      """.stripMargin)
    generatedCode should matchSnippet(expectedCode)

  }
}
