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

import dbis.piglet.tools.CodeMatchers
import dbis.piglet.tools.TestTools._
import dbis.piglet.parser.PigParser.parseScript
import dbis.piglet.Piglet._
import dbis.piglet.op._
import dbis.piglet.expr._
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.plan.rewriting.Rewriter._
import dbis.piglet.plan.rewriting.Rules
import dbis.piglet.schema._
import dbis.piglet.udf.UDFTable
import org.scalatest.FlatSpec
import dbis.piglet.backends.BackendManager
import org.scalatest.{ Matchers, BeforeAndAfterAll, FlatSpec }
import dbis.piglet.plan.rewriting.Rules
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.codegen.CodeGenTarget

class FlinkCompileSpec extends FlatSpec with BeforeAndAfterAll with Matchers with CodeMatchers {

  override def beforeAll() {
    Rules.registerAllRules()
  }
  def cleanString(s: String): String = s.stripLineEnd.replaceAll("""\s+""", " ").trim
  
  val backendConf = BackendManager.init("flink")
  val codeGenerator = new FlinkCodeGenStrategy()

  "The compiler output" should "contain the Flink header & footer" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val generatedCode = cleanString(codeGenerator.emitImport(ctx)
      + codeGenerator.emitHeader1(ctx, "test")
      + codeGenerator.emitHeader2(ctx, "test")
      + codeGenerator.emitFooter(ctx, new DataflowPlan(List.empty[PigOperator])))
    val expectedCode = cleanString("""
      |import org.apache.flink.api.scala._
      |import dbis.piglet.backends.flink._
      |import dbis.piglet.backends.{SchemaClass, Record}
      |import org.apache.flink.util.Collector
      |import org.apache.flink.api.common.operators.Order
      |import dbis.piglet.backends.flink.Sampler._
      |
      |object test {
      |    val env = ExecutionEnvironment.getExecutionEnvironment
      |    def main(args: Array[String]) {
      |    }
      |}
    """.stripMargin)
    //         |        env.execute("Starting Query")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val file = new java.io.File(".").getCanonicalPath + "/file.csv"

    val op = Load(Pipe("a"), file)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString(s"""val a = PigStorage[Record]().load(env, "$file", (data: Array[String]) => Record(data))""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD with PigStorage" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val file = new java.io.File(".").getCanonicalPath + "/file.csv"

    val op = Load(Pipe("a"), file, None, Some("PigStorage"), List("""','"""))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString(s"""val a = PigStorage[Record]().load(env, "$file", (data: Array[String]) => Record(data), ',')""")
    assert(generatedCode == expectedCode)
  }
  /*
  it should "contain code for LOAD with RDFFileStorage" in {
    
    val file = new java.io.File(".").getCanonicalPath + "/file.n3"
    
    val op = Load(Pipe("a"), file, None, Some("RDFFileStorage"))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    
    val expectedCode = cleanString(s"""val a = RDFFileStorage().load(env, "${file}")""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for FILTER" in { 
    val op = Filter(Pipe("a"), Pipe("b"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))) 
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op)) 
    val expectedCode = cleanString("val a = b.filter(t => {t(1) < 42})") 
    assert(generatedCode == expectedCode) 
  }
  */
  it should "contain code for DUMP" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val op = Dump(Pipe("a"))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""a.map(_.mkString()).print""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for STORE" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val file = new java.io.File(".").getCanonicalPath + "/file.csv"
    val op = Store(Pipe("A"), file)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString(
      s"""PigStorage[Record]().write("$file", A) env.execute("Starting Query")""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for FILTER" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val op = Filter(Pipe("aa"), Pipe("bb"), Lt(RefExpr(PositionalField(1)), RefExpr(Value(42))))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val aa = bb.filter{t => t.get(1) < 42}")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a complex FILTER" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val ops = parseScript(
      """b = LOAD 'file' AS (x: double, y:double, z1:int, z2: int);
        |c = FILTER b BY x > 0 AND (y < 0 OR (NOT z1 == z2));""".stripMargin)
    val plan = new DataflowPlan(ops)
    val op = ops(1)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val c = b.filter{t => t._0 > 0 && (t._1 < 0 || (!(t._2 == t._3)))}")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a filter with a function expression" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    Schema.init()
    val op = Filter(Pipe("a"), Pipe("b"), Gt(
      Func("aFunc", List(RefExpr(PositionalField(0)), RefExpr(PositionalField(1)))),
      RefExpr(Value("0"))))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val a = b.filter{t => aFunc(t.get(0),t.get(1)) > 0}")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a filter with a function expression and boolean" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    Schema.init()
    val op = Filter(Pipe("a"), Pipe("b"), And(
      Eq(Func("aFunc", List(RefExpr(NamedField("x")), RefExpr(NamedField("y")))), RefExpr(Value(true))),
      Geq(Func("cFunc", List(RefExpr(NamedField("x")), RefExpr(NamedField("y")))), RefExpr(NamedField("x")))), windowMode = false)
    op.schema = Some(Schema(Array(Field("x", Types.IntType),
      Field("y", Types.DoubleType))))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val a = b.filter{t => aFunc(t._0,t._1) == true && cFunc(t._0,t._1) >= t._0}
      |""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for DISTINCT" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val op = Distinct(Pipe("a"), Pipe("b"))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val a = b.distinct")
    assert(generatedCode == expectedCode)
  }
  it should "contain code for LIMIT" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val op = Limit(Pipe("a"), Pipe("b"), 10)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val a = b.first(10)")
    assert(generatedCode == expectedCode)
  }

  it should "contain code a foreach statement with function expressions" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    Schema.init()
    // a = FOREACH b GENERATE TOMAP("field1", $0, "field2", $1);
    val op = Foreach(Pipe("aa"), Pipe("bb"), GeneratorList(List(
      GeneratorExpr(Func("TOMAP", List(
        RefExpr(Value("\"field1\"")),
        RefExpr(PositionalField(0)),
        RefExpr(Value("\"field2\"")),
        RefExpr(PositionalField(1))))))))
    op.constructSchema
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val aa = bb.map{t => _t$1_Tuple(PigFuncs.toMap(\"field1\",t.get(0),\"field2\",t.get(1)))}")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a foreach statement with another function expression" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    Schema.init()
    // a = FOREACH b GENERATE $0, COUNT($1) AS CNT;
    val op = Foreach(Pipe("aa"), Pipe("bb"), GeneratorList(List(
      GeneratorExpr(RefExpr(PositionalField(0))),
      GeneratorExpr(Func("COUNT", List(RefExpr(PositionalField(1)))), Some(Field("CNT", Types.LongType))))))
    op.constructSchema
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val aa = bb.map{t => _t$1_Tuple(t.get(0), PigFuncs.count(t.get(1)))}")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a foreach statement with a UDF expression" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    // aa = FOREACH bb GENERATE $0, distance($1, $2, 1.0, 2.0) AS dist;
    val ops = parseScript(
      """
      |bb = LOAD 'file' AS (f1: int, f2: int, f3: int);
      |aa = FOREACH bb GENERATE $0, Distances.spatialDistance($1, $2, 1.0, 2.0) AS dist;
    """.stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("aa").get
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val aa = bb.map{t => _t$1_Tuple(t._0, Distances.spatialDistance(t._1,t._2,1.0,2.0))}")
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
    val ctx = CodeGenContext(CodeGenTarget.Flink, Some(plan.udfAliases.toMap))
    // op.constructSchema
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("val aa = bb.map{t => _t$1_Tuple(t._0, Distances.spatialDistance(t._1,t._2,1.0,2.0))}")
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for deref operator on maps in foreach statement" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val ops = parseScript(
      """
      |in = LOAD 'file' AS (s1: chararray, s2: chararray);
      |b = FOREACH in GENERATE ["k1", s1] as map1, ["k2", s2] as map2;
      |a = FOREACH b GENERATE $0#"k1", $1#"k2";
    """.stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("a").get

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val a = b.map{t => _t$1_Tuple(t._0("k1"), t._1("k2"))}""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for deref operator on tuple in foreach statement" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val ops = parseScript(
      """
        |in = LOAD 'file' AS (s1: int, s2: int, s3: int);
        |b = FOREACH in GENERATE ("k1", s1) as t1, ("k2", s2) as t2, ("k3", s3) as t3;
        |a = FOREACH b GENERATE $0.$1, $2.$0;
      """.stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("a").get

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
        |val a = b.map{t => _t$1_Tuple(t._0._1, t._2._0)}""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for a nested foreach statement" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
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
    //println("schema = " + foreachOp.schema)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, foreachOp))

    val expectedCode = cleanString(
      """val uniqcnt = grpd.map{t => {
        |val sym = t._1.map(l => l._1).toList
        |val uniq_sym = sym.distinct
        |_t$1_Tuple(t._0, PigFuncs.count(uniq_sym))}}""".stripMargin)

    generatedCode should matchSnippet(expectedCode)
    val schemaClassCode = cleanString(codeGenerator.emitSchemaHelpers(ctx, List(foreachOp.schema.get)))
  }

  it should "contain code for a foreach statement with constructors for tuple, bag, and map" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val ops = parseScript(
      """data = load 'file' as (f1: int, f2: int, name:chararray);
        |out = foreach data generate (f1, f2), {f1, f2}, [name, f1];""".stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("out").get
    val schemaClassCode = cleanString(codeGenerator.emitSchemaHelpers(ctx, List(op.schema.get)))

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    //println("schema class = " + schemaClassCode)

    val expectedCode = cleanString(
      """val out = data.map{t => _t$1_Tuple(_t$2_Tuple(t._0,t._1), List(_t$3_Tuple(t._0),_t$3_Tuple(t._1)),
        |Map[String,Int](t._2 -> t._0))}""".stripMargin)

    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for the stream through statement without parameters" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val ops = parseScript(
      """data = load 'data.csv' as (f1: int, f2: int);
        |res = STREAM data THROUGH myOp();""".stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("res").get

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString(
      """val data_helper = data.map(t => List(t._0, t._1))
        |val res = myOp(env, data_helper).map(t => _t$1_Tuple(t(0).asInstanceOf[Int], t(1).asInstanceOf[Int]))
        |""".stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }

  it should "contain code for the stream through statement with parameters" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val ops = parseScript(
      """data = load 'data.csv' as (f1: int, f2: int);
        |res = STREAM data THROUGH package.myOp(1, 42.0);""".stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("res").get

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString(
      """val data_helper = data.map(t => List(t._0, t._1))
        |val res = package.myOp(env, data_helper,1,42.0).map(t => _t1_Tuple(t(0).asInstanceOf[Int], t(1).asInstanceOf[Int]))
        |""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a UNION operator on two relations" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    // a = UNION b, c;
    val op = Union(Pipe("a"), List(Pipe("b"), Pipe("c")))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
        |val a = b.union(c)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a UNION operator on more than two relations" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    // a = UNION b, c, d;
    val op = Union(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
        |val a = b.union(c).union(d)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for GROUP BY ALL" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val ops = parseScript(
      """
        |bb = LOAD 'file.csv' USING PigStorage(',') AS (f1: int, f2: chararray, f3: double);
        |aa = GROUP bb ALL;
      """.stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("aa").get
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""val aa = bb.setParallelism(1).mapPartition( (in, out: Collector[_t2_Tuple]) =>  out.collect(_t2_Tuple("all", in.toIterable)))""")
    assert(generatedCode == expectedCode)

    val schemaCode = cleanString(codeGenerator.emitSchemaHelpers(ctx, List(op.schema.get)))
    val expectedSchemaCode =
      cleanString("""
         |case class _t2_Tuple (_0: String, _1: Iterable[_t1_Tuple]) extends java.io.Serializable with SchemaClass {
         |  override def mkString(_c: String = ",") = _0 + _c + "{" + _1.mkString(",") + "}"
         |}
         |implicit def convert_t2_Tuple(t: (String, Iterable[_t1_Tuple])): _t2_Tuple = _t2_Tuple(t._1, t._2)
       """.stripMargin)
    assert(schemaCode == expectedSchemaCode)
  }

  it should "contain code for GROUP BY $0" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    ctx.set("tuplePrefix", "t")
    val ops = parseScript(
      """
        |bb = LOAD 'file.csv' USING PigStorage(',') AS (f1: int, f2: chararray, f3: double);
        |aa = GROUP bb BY f1;
      """.stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("aa").get
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val aa = bb.groupBy(t => t._0).reduceGroup{ (in, out: Collector[_t2_Tuple]) => val itr = in.toIterable; out.collect(_t2_Tuple(itr.head._0, itr)) }""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for GROUP BY with multiple keys" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    ctx.set("tuplePrefix", "t")
    val ops = parseScript(
      """
        |bb = LOAD 'file.csv' USING PigStorage(',') AS (f1: int, f2: chararray, f3: double);
        |aa = GROUP bb BY ($0, $1);
      """.stripMargin)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("aa").get
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val aa = bb.groupBy(t => (t._0,t._1)).reduceGroup{ (in, out: Collector[_t3_Tuple]) => val itr = in.toIterable; out.collect(_t3_Tuple(_t2_Tuple(itr.head._0,itr.head._1), itr)) }""".stripMargin)
    val schemaClassCode = cleanString(codeGenerator.emitSchemaHelpers(ctx, List(op.schema.get)))
    assert(generatedCode == expectedCode)
  }
  it should "contain code for simple ORDER BY" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    // aa = ORDER bb BY $0
    val op = OrderBy(Pipe("aa"), Pipe("bb"), List(OrderBySpec(PositionalField(0), OrderByDirection.AscendingOrder)))
    val schema = Schema(Array(Field("f1", Types.IntType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))

    op.schema = Some(schema)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
        |val aa = bb.setParallelism(1).sortPartition(0, Order.ASCENDING)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for complex ORDER BY" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    Schema.init()
    // a = ORDER b BY f1, f3
    val op = OrderBy(Pipe("a"), Pipe("b"), List(OrderBySpec(NamedField("f1"), OrderByDirection.DescendingOrder),
      OrderBySpec(NamedField("f3"), OrderByDirection.AscendingOrder)))
    val schema = Schema(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))

    op.schema = Some(schema)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
        |val a = b.setParallelism(1).sortPartition(0, Order.DESCENDING).sortPartition(2, Order.ASCENDING)""".stripMargin)
    assert(generatedCode == expectedCode)
  }
  
  it should "contain code for the sample operator with a literal value" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    // aa = SAMPLE bb 0.01;
    val op = Sample(Pipe("aa"), Pipe("bb"), RefExpr(Value(0.01)))
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
        |val aa = bb.sample(false, 0.01)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a binary join statement with simple expression" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    Schema.init()
    val op = Join(Pipe("aa"), List(Pipe("bb"), Pipe("cc")), List(List(PositionalField(0)), List(PositionalField(0))))
    val schema = Schema(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))
    val input1 = Pipe("bb", Load(Pipe("bb"), "input/file.csv", Some(schema), Some("PigStorage"), List("\",\"")))
    val input2 = Pipe("cc", Load(Pipe("cc"), "input/file.csv", Some(schema), Some("PigStorage"), List("\",\"")))
    op.inputs = List(input1, input2)
    op.constructSchema

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val aa = bb.join(cc).where("_0").equalTo("_0").map{ t => val (v1,v2) = t _t2_Tuple(v1._0, v1._1, v1._2, v2._0, v2._1, v2._2) }""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a binary join statement with expression lists" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    Schema.init()
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c")), List(List(PositionalField(0), PositionalField(1)),
      List(PositionalField(1), PositionalField(2))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))))
    val input1 = Pipe("b", Load(Pipe("b"), "input/file.csv", Some(schema), Some("PigStorage"), List("\",\"")))
    val input2 = Pipe("c", Load(Pipe("c"), "input/file.csv", Some(schema), Some("PigStorage"), List("\",\"")))
    op.inputs = List(input1, input2)
    op.constructSchema

    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
     |val a = b.join(c).where("_0","_1").equalTo("_1","_2").map{ t => val (v1,v2) = t _t2_Tuple(v1._0, v1._1, v1._2, v2._0, v2._1, v2._2) }""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a multiway join statement" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    Schema.init()
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")), List(List(PositionalField(0)),
      List(PositionalField(0)), List(PositionalField(0))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)))))
    val input1 = Pipe("b", Load(Pipe("b"), "input/file.csv", Some(schema), Some("PigStorage"), List("\",\"")))
    val input2 = Pipe("c", Load(Pipe("c"), "input/file.csv", Some(schema), Some("PigStorage"), List("\",\"")))
    val input3 = Pipe("d", Load(Pipe("d"), "input/file.csv", Some(schema), Some("PigStorage"), List("\",\"")))
    op.inputs = List(input1, input2, input3)
    op.constructSchema
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
      |val a = b.join(c).where("_0").equalTo("_0").join(d).where("_1._0").equalTo("_0").map{ t => val ((v1,v2),v3) = t _t2_Tuple(v1._0, v1._1, v1._2, v2._0, v2._1, v2._2, v3._0, v3._1, v3._2) }""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for multiple joins" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    Schema.init()
    val ops = parseScript(
      """a = load 'file' as (a: chararray);
        |b = load 'file' as (a: chararray);
        |c = load 'file' as (a: chararray);
        |j1 = join a by $0, b by $0;
        |j2 = join a by $0, c by $0;
        |j = join j1 by $0, j2 by $0;
        |""".stripMargin)
    val plan = new DataflowPlan(ops)
    val generatedCode1 = cleanString(codeGenerator.emitNode(ctx, plan.findOperatorForAlias("j1").get))
    val generatedCode2 = cleanString(codeGenerator.emitNode(ctx, plan.findOperatorForAlias("j2").get))
    val generatedCode3 = cleanString(codeGenerator.emitNode(ctx, plan.findOperatorForAlias("j").get))

    val finalJoinOp = plan.findOperatorForAlias("j").get
    // TODO: Schema classes!!!!
    val schemaClassCode = cleanString(codeGenerator.emitSchemaHelpers(ctx, List(finalJoinOp.schema.get)))

    val expectedCode1 = cleanString(
      """val j1 = a.join(b).where("_0").equalTo("_0").map{ t => val (v1,v2) = t _t2_Tuple(v1._0, v2._0) }""".stripMargin)
    assert(generatedCode1 == expectedCode1)

    val expectedCode2 = cleanString(
      """val j2 = a.join(c).where("_0").equalTo("_0").map{ t => val (v1,v2) = t _t3_Tuple(v1._0, v2._0) }""".stripMargin)
    assert(generatedCode2 == expectedCode2)

    val expectedCode3 = cleanString(
      """ val j = j1.join(j2).where("_0").equalTo("_0").map{ t => val (v1,v2) = t _t4_Tuple(v1._0, v1._1, v2._0, v2._1) }""".stripMargin)
    assert(generatedCode3 == expectedCode3)
  }

  it should "contain code for a simple accumulate statement" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val ops = parseScript("b = load 'file'; a = ACCUMULATE b GENERATE COUNT($0), AVG($1), SUM($2);")
    val schema = Schema(Array(Field("t1", Types.IntType),
      Field("t2", Types.IntType),
      Field("t3", Types.IntType)))
    ops.head.schema = Some(schema)
    val plan = new DataflowPlan(ops)
    val op = plan.findOperatorForAlias("a").get
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString("""
       |val a = b.aggregate(Aggregations.COUNT,0).and(Aggregations.AVG,1).and(Aggregations.SUM,2)""".stripMargin)

    assert(generatedCode == expectedCode)

  }

  it should "not contain code for EMPTY operators" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val op = Empty(Pipe("_"))

    assert(codeGenerator.emitNode(ctx, op) == "")
  }

  it should "contain embedded code" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val ops = parseScript(
      """
        |<% def someFunc(s: String): String = {
        | s
        |}
        |%>
        |A = LOAD 'file.csv';
      """.stripMargin)
    val plan = new DataflowPlan(ops)
    val theCode = codeGenerator.emitHeader1(ctx, "test") + codeGenerator.emitEmbeddedCode(ctx, plan.code)
    assert(cleanString(theCode) ==
      cleanString("""
        |object test {
        |  val env = ExecutionEnvironment.getExecutionEnvironment
        |def someFunc(s: String): String = {
        | s
        |}
      """.stripMargin))
    val udf = UDFTable.findUDF("someFunc", Types.AnyType)
    udf shouldBe defined
  }

  it should "contain code for macros" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val ops = parseScript(
      """
      |DEFINE my_macro(in_alias, p) RETURNS out_alias {
      |$out_alias = FOREACH $in_alias GENERATE $0 + $p;
      |};
      |
      |in = LOAD 'file' AS (i: double);
      |out = my_macro(in, 42);
      |DUMP out;
    """.stripMargin)
    val plan = new DataflowPlan(ops)
    val rewrittenPlan = rewritePlan(plan)
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, rewrittenPlan.findOperatorForAlias("out").get))
    val expectedCode = cleanString(
      """
        |val out = in.map{t => _t2_Tuple(t._0 + 42)}
        |""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for multiple macros" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
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
      """.stripMargin)
    val plan = new DataflowPlan(ops)
    val rewrittenPlan = rewritePlan(plan)
    val generatedCode1 = cleanString(codeGenerator.emitNode(ctx, rewrittenPlan.findOperatorForAlias("out").get))
    val expectedCode1 = cleanString(
      """
        |val out = in.map{t => _t4_Tuple(t._0 + 42, t._1)}
        |""".stripMargin)
    assert(generatedCode1 == expectedCode1)
    val generatedCode2 = cleanString(codeGenerator.emitNode(ctx, rewrittenPlan.findOperatorForAlias("out2").get))
    val expectedCode2 = cleanString(
      """
        |val out2 = out.map{t => _t4_Tuple(t._0, t._1 - 5)}
        |""".stripMargin)
    assert(generatedCode2 == expectedCode2)
  }

  it should "contain code for invoking a macro multiple times" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
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
      """.stripMargin)
    val plan = new DataflowPlan(ops)
    val rewrittenPlan = rewritePlan(plan)
    val generatedCode1 = cleanString(codeGenerator.emitNode(ctx, rewrittenPlan.findOperatorForAlias("out").get))
    val expectedCode1 = cleanString(
      """
        |val out = in.map{t => _t3_Tuple(t._0 + 42)}
        |""".stripMargin)
    assert(generatedCode1 == expectedCode1)
    val generatedCode2 = cleanString(codeGenerator.emitNode(ctx, rewrittenPlan.findOperatorForAlias("out2").get))
    val expectedCode2 = cleanString(
      """
        |val out2 = out.map{t => _t3_Tuple(t._0 + 43)}
        |""".stripMargin)
    assert(generatedCode2 == expectedCode2)
  }

  it should "contain code for schema classes" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val ops = parseScript(
      """
      |A = LOAD 'file' AS (f1: int, f2: chararray, f3: double);
      |B = FILTER A BY f1 > 0;
      |C = FOREACH B GENERATE f1, f2, f3 + 5, $2 + 44 AS f4:int;
      |DUMP C;
    """.stripMargin)
    val plan = new DataflowPlan(ops)
    val rewrittenPlan = rewritePlan(plan)

    var code: String = ""
    for (schema <- Schema.schemaList()) {
      code = code + codeGenerator.emitSchemaHelpers(ctx, List(schema))
    }

    val generatedCode = cleanString(code)
    val expectedCode = cleanString(
      """
      |case class _t2_Tuple (_0: Int, _1: String, _2: Double, _3: Int) extends java.io.Serializable with SchemaClass {
      |override def mkString(_c: String = ",") = _0 + _c + _1 + _c + _2 + _c + _3
      |}
      |implicit def convert_t2_Tuple(t: (Int, String, Double, Int)): _t2_Tuple = _t2_Tuple(t._1, t._2, t._3, t._4)
      |case class _t1_Tuple (_0: Int, _1: String, _2: Double) extends java.io.Serializable with SchemaClass {
      |override def mkString(_c: String = ",") = _0 + _c + _1 + _c + _2
      |}
      |implicit def convert_t1_Tuple(t: (Int, String, Double)): _t1_Tuple = _t1_Tuple(t._1, t._2, t._3)
      |""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for nested schema classes" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val ops = parseScript(
      """
        |daily = load 'file' using PigStorage(',') as (exchange: chararray, symbol: chararray);
        |grpd  = group daily by exchange;
        |DUMP grpd;
      """.stripMargin)
    val plan = new DataflowPlan(ops)
    val rewrittenPlan = rewritePlan(plan)

    var code: String = ""
    for (schema <- Schema.schemaList()) {
      code = code + codeGenerator.emitSchemaHelpers(ctx, List(schema))
    }

    val generatedCode = cleanString(code)
    val expectedCode = cleanString(
      """
        |case class _t2_Tuple (_0: String, _1: Iterable[_t1_Tuple]) extends java.io.Serializable with SchemaClass {
        |  override def mkString(_c: String = ",") = _0 + _c + "{" + _1.mkString(",") + "}"
        |}
        |implicit def convert_t2_Tuple(t: (String, Iterable[_t1_Tuple])): _t2_Tuple = _t2_Tuple(t._1, t._2)
        |case class _t1_Tuple (_0: String, _1: String) extends java.io.Serializable with SchemaClass {
        |  override def mkString(_c: String = ",") = _0 + _c + _1
        |}
        |implicit def convert_t1_Tuple(t: (String, String)): _t1_Tuple = _t1_Tuple(t._1, t._2)
        |""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code to handle LOAD with PigStorage but without an explicit schema" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val ops = parseScript(
      """
      |in = load 'file' using PigStorage(':');
      |out = filter in by $1 == "root";
      |dump out;
    """.stripMargin)
    val plan = new DataflowPlan(ops)
    val rewrittenPlan = rewritePlan(plan)
    val op = rewrittenPlan.findOperatorForAlias("out").get
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString(
      """
      |val out = in.filter{t => t.get(1) == "root"}
    """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain correct code for a function call with bytearray parameters" in {
    val ctx = CodeGenContext(CodeGenTarget.Flink)
    val ops = parseScript(
      """
      |in = load 'file' as (x, y);
      |in2 = foreach in generate x, y;
      |out = foreach in2 generate tokenize(x);
      |dump out;
    """.stripMargin)
    val plan = new DataflowPlan(ops)
    val rewrittenPlan = rewritePlan(plan)
    val op = rewrittenPlan.findOperatorForAlias("out").get
    val generatedCode = cleanString(codeGenerator.emitNode(ctx, op))
    val expectedCode = cleanString(
      """
        |val out = in2.map{t => _t$1_Tuple(PigFuncs.tokenize(t._0.asInstanceOf[String]).map(_t$2_Tuple(_)))}
      """.stripMargin)
    generatedCode should matchSnippet(expectedCode)
  }
}

