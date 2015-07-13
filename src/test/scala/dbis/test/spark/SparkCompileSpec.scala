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

import dbis.pig.PigCompiler._
import dbis.pig.codegen.ScalaBackendGenCode
import dbis.pig.op._
import dbis.pig.plan.DataflowPlan
import dbis.pig.schema._
import org.scalatest.FlatSpec

class SparkCompileSpec extends FlatSpec {
  def cleanString(s: String) : String = s.stripLineEnd.replaceAll("""\s+""", " ").trim
  val templateFile = "src/main/resources/spark-template.stg"
  "The compiler output" should "contain the Spark header & footer" in {
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitImport
      + codeGenerator.emitHeader1("test")
      + codeGenerator.emitHeader2("test")
      + codeGenerator.emitFooter)
//        |import dbis.spark._
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
    val op = Load(Pipe("a"), "file.csv")
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val file = new java.io.File(".").getCanonicalPath + "/file.csv"
    val expectedCode = cleanString(s"""val a = PigStorage().load(sc, "${file}")""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD with PigStorage" in {
    val op = Load(Pipe("a"), "file.csv", None, "PigStorage", List("""','"""))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val file = new java.io.File(".").getCanonicalPath + "/file.csv"
    val expectedCode = cleanString(s"""val a = PigStorage().load(sc, "${file}", ',')""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD with RDFFileStorage" in {
    val op = Load(Pipe("a"), "file.n3", None, "RDFFileStorage")
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val file = new java.io.File(".").getCanonicalPath + "/file.n3"
    val expectedCode = cleanString(s"""val a = RDFFileStorage().load(sc, "${file}")""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for FILTER" in {
    val op = Filter(Pipe("aa"), Pipe("bb"), Lt(RefExpr(PositionalField(1)), RefExpr(Value("42"))))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = bb.filter(t => {t(1) < 42})")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a complex FILTER" in {
    val ops = parseScript("b = LOAD 'file' AS (x: double, y:double, z1:int, z2: int); c = FILTER b BY x > 0 AND (y < 0 OR (NOT z1 == z2));")
    val plan = new DataflowPlan(ops)
    val op = ops(1)
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val c = b.filter(t => {t(0).toDouble > 0 && (t(1).toDouble < 0 || (!(t(2).toInt == t(3).toInt)))})")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for DUMP" in {
    val op = Dump(Pipe("a"))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""a.collect.map(t => println(t.mkString(",")))""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for STORE" in {
    val op = Store(Pipe("A"), "file.csv")
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val file = new java.io.File(".").getCanonicalPath + "/file.csv"
//    val expectedCode = cleanString(s"""A.map(t => tupleAToString(t)).coalesce(1, true).saveAsTextFile("${file}")""")
    val expectedCode = cleanString(s"""val A_storehelper = A.map(t => tupleAToString(t)).coalesce(1, true) PigStorage().write("$file", A_storehelper)""")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for the STORE helper function" in {
    val op = Store(Pipe("A"), "file.csv")
    op.schema = Some(new Schema(BagType(TupleType(Array(
      Field("f1", Types.IntType),
      Field("f2", BagType(TupleType(Array(Field("f3", Types.DoubleType), Field("f4", Types.DoubleType))), "b"))
    ), "t"), "s")))

    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitHelperClass(op))
    val expectedCode = cleanString("""
        |def tupleAToString(t: List[Any]): String = {
        |implicit def anyToSeq(a: Any) = a.asInstanceOf[Seq[Any]]
        |val sb = new StringBuilder
        |sb.append(t(0))
        |.append(",")
        |.append(t(1).map(s => s.mkString("(", ",", ")")).mkString("{", ",", "}"))
        |sb.toString
        |}""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for GROUP BY ALL" in {
    val op = Grouping(Pipe("aa"), Pipe("bb"), GroupingExpression(List()))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = bb.glom")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for GROUP BY $0" in {
    val op = Grouping(Pipe("aa"), Pipe("bb"), GroupingExpression(List(PositionalField(0))))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = bb.groupBy(t => {t(0)}).map{case (k,v) => List(k,v)}")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for GROUP BY with multiple keys" in {
    val op = Grouping(Pipe("aa"), Pipe("bb"), GroupingExpression(List(PositionalField(0), PositionalField(1))))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = bb.groupBy(t => {(t(0),t(1))}).map{case (k,v) => List(k,v)}")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for DISTINCT" in {
    val op = Distinct(Pipe("aa"), Pipe("bb"))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = bb.distinct")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for Limit" in {
    val op = Limit(Pipe("aa"), Pipe("bb"), 10)
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = sc.parallelize(bb.take(10))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a binary join statement with simple expression" in {
    val op = Join(Pipe("aa"), List(Pipe("bb"), Pipe("cc")), List(List(PositionalField(0)), List(PositionalField(0))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
                                                              Field("f2", Types.DoubleType),
                                                              Field("f3", Types.IntType)), "t"), "s"))
    val input1 = Pipe("bb",Load(Pipe("bb"), "file.csv", Some(schema), "PigStorage", List("\",\"")))
    val input2 = Pipe("cc",Load(Pipe("cc"), "file.csv", Some(schema), "PigStorage", List("\",\"")))
    op.inputs = List(input1,input2)
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val bb_kv = bb.map(t => (t(0),t))
      |val cc_kv = cc.map(t => (t(0),t))
      |val aa = bb_kv.join(cc_kv).map{case (k,(v,w)) => (k, v ++ w)}.map{case (k,v) => v}""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a binary join statement with expression lists" in {
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c")), List(List(PositionalField(0), PositionalField(1)),
      List(PositionalField(1), PositionalField(2))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
                                                              Field("f2", Types.DoubleType),
                                                              Field("f3", Types.IntType)), "t"), "s"))
    val input1 = Pipe("b",Load(Pipe("b"), "file.csv", Some(schema), "PigStorage", List("\",\"")))
    val input2 = Pipe("c",Load(Pipe("c"), "file.csv", Some(schema), "PigStorage", List("\",\"")))
    op.inputs=List(input1,input2)
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val b_kv = b.map(t => (Array(t(0),t(1)).mkString,t))
      |val c_kv = c.map(t => (Array(t(1),t(2)).mkString,t))
      |val a = b_kv.join(c_kv).map{case (k,(v,w)) => (k, v ++ w)}.map{case (k,v) => v}""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a multiway join statement" in {
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")), List(List(PositionalField(0)),
      List(PositionalField(0)), List(PositionalField(0))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
                                                              Field("f2", Types.DoubleType),
                                                              Field("f3", Types.IntType)), "t"), "s"))
    val input1 = Pipe("b",Load(Pipe("b"), "file.csv", Some(schema), "PigStorage", List("\",\"")))
    val input2 = Pipe("c",Load(Pipe("c"), "file.csv", Some(schema), "PigStorage", List("\",\"")))
    val input3 = Pipe("d",Load(Pipe("d"), "file.csv", Some(schema), "PigStorage", List("\",\"")))
    op.inputs=List(input1,input2,input3)
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val b_kv = b.map(t => (t(0),t))
      |val c_kv = c.map(t => (t(0),t))
      |val d_kv = d.map(t => (t(0),t))
      |val a = b_kv.join(c_kv).map{case (k,(v,w)) => (k, v ++ w)}.join(d_kv).map{case (k,(v,w)) => (k, v ++ w)}.map{case (k,v) => v}""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code a foreach statement with function expressions" in {
    // a = FOREACH b GENERATE TOMAP("field1", $0, "field2", $1);
    val op = Foreach(Pipe("aa"), Pipe("bb"), GeneratorList(List(
      GeneratorExpr(Func("TOMAP", List(
        RefExpr(Value("\"field1\"")),
        RefExpr(PositionalField(0)),
        RefExpr(Value("\"field2\"")),
        RefExpr(PositionalField(1)))))
    )))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = bb.map(t => List(PigFuncs.toMap(\"field1\",t(0),\"field2\",t(1))))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a foreach statement with another function expression" in {
    // a = FOREACH b GENERATE $0, COUNT($1) AS CNT;
    val op = Foreach(Pipe("aa"), Pipe("bb"), GeneratorList(List(
        GeneratorExpr(RefExpr(PositionalField(0))),
        GeneratorExpr(Func("COUNT", List(RefExpr(PositionalField(1)))), Some(Field("CNT", Types.LongType)))
      )))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = bb.map(t => List(t(0),PigFuncs.count(t(1).asInstanceOf[Seq[Any]])))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a foreach statement with a UDF expression" in {
    // aa = FOREACH bb GENERATE $0, distance($1, $2, 1.0, 2.0) AS dist;
    val plan = parseScript("aa = FOREACH bb GENERATE $0, Distances.spatialDistance($1, $2, 1.0, 2.0) AS dist;")
    val op = plan.head
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("val aa = bb.map(t => List(t(0),Distances.spatialDistance(t(1),t(2),1.0,2.0)))")
    assert(generatedCode == expectedCode)
  }

  it should "contain code for deref operator on maps in foreach statement" in {
    // a = FOREACH b GENERATE $0#"k1", $1#"k2";
    val op = Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(GeneratorExpr(RefExpr(DerefMap(PositionalField(0), "\"k1\""))),
      GeneratorExpr(RefExpr(DerefMap(PositionalField(1), "\"k2\""))))))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      |val a = b.map(t => List(t(0).asInstanceOf[Map[String,Any]]("k1"),t(1).asInstanceOf[Map[String,Any]]("k2")))""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for deref operator on tuple in foreach statement" in {
    // a = FOREACH b GENERATE $0.$1, $2.$0;
    val op = Foreach(Pipe("a"), Pipe("b"), GeneratorList(List(GeneratorExpr(RefExpr(DerefTuple(PositionalField(0), PositionalField(1)))),
      GeneratorExpr(RefExpr(DerefTuple(PositionalField(2), PositionalField(0)))))))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.map(t => List(t(0).asInstanceOf[List[Any]](1),t(2).asInstanceOf[List[Any]](0)))""".stripMargin)
    assert(generatedCode == expectedCode)
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
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(plan.findOperatorForAlias("uniqcnt").get))

    val expectedCode = cleanString(
      """val uniqcnt = grpd.map(t => { val sym = t(1).asInstanceOf[Seq[Any]].map(l => l.asInstanceOf[Seq[Any]](1))
        |val uniq_sym = sym.distinct ( List(t(0),PigFuncs.count(uniq_sym.asInstanceOf[Seq[Any]])) )})""".stripMargin)

    assert(generatedCode == expectedCode)
  }

  it should "contain code for a foreach statement with constructors for tuple, bag, and map" in {
    val ops = parseScript(
      """data = load 'file' as (f1: int, f2: int, name:chararray);
        |out = foreach data generate (f1, f2), {f1, f2}, [name, f1];""".stripMargin)
    val plan = new DataflowPlan(ops)
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(plan.findOperatorForAlias("out").get))

    val expectedCode = cleanString(
      """val out = data.map(t => List(PigFuncs.toTuple(t(0).toInt,t(1).toInt),PigFuncs.toBag(t(0).toInt,t(1).toInt),PigFuncs.toMap(t(2).toString,t(0).toInt)))""".stripMargin)

    assert(generatedCode == expectedCode)
  }

  it should "contain code for a union operator on two relations" in {
    // a = UNION b, c;
    val op = Union(Pipe("aa"), List(Pipe("bb"), Pipe("cc")))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val aa = bb.union(cc)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a union operator on more than two relations" in {
    // a = UNION b, c, d;
    val op = Union(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.union(c).union(d)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for the sample operator with a literal value" in {
    // aa = SAMPLE bb 0.01;
    val op = Sample(Pipe("aa"), Pipe("bb"), RefExpr(Value("0.01")))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val aa = bb.sample(0.01)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for the sample operator with an expression" in {
    // a = SAMPLE b 100 / $3
    val op = Sample(Pipe("a"), Pipe("b"), Div(RefExpr(Value("100")), RefExpr(PositionalField(3))))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.sample(100 / t(3))""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for the stream through statement without parameters" in {
    // aa = STREAM bb THROUGH myOp
    val op = StreamOp(Pipe("aa"), Pipe("bb"), "myOp")
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val aa = myOp(bb)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for the stream through statement with parameters" in {
    // a = STREAM b THROUGH package.myOp(1, 42.0)
    val op = StreamOp(Pipe("a"), Pipe("b"), "package.myOp", Some(List(Value("1"), Value(42.0))))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = package.myOp(b,1,42.0)""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for simple ORDER BY" in {
    // aa = ORDER bb BY $0
    val op = OrderBy(Pipe("aa"), Pipe("bb"), List(OrderBySpec(PositionalField(0), OrderByDirection.AscendingOrder)))
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val aa = bb.keyBy(t => t(0)).sortByKey(true).map{case (k,v) => v}""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for complex ORDER BY" in {
    // a = ORDER b BY f1, f3
    val op = OrderBy(Pipe("a"), Pipe("b"), List(OrderBySpec(NamedField("f1"), OrderByDirection.AscendingOrder),
                                    OrderBySpec(NamedField("f3"), OrderByDirection.AscendingOrder)))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
                                                              Field("f2", Types.DoubleType),
                                                              Field("f3", Types.IntType)
    ), "t"), "s"))

    op.schema = Some(schema)
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
        |val a = b.keyBy(t => custKey_a_b(t(0).toString,t(2).toInt)).sortByKey(true).map{case (k,v) => v}""".stripMargin)
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
    val ops = parseScript("b = load 'file'; a = foreach b generate $0, flatten($1);")
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
                                                              Field("f2", TupleType(Array(Field("f3", Types.IntType)), "t2"))
    ), "t"), "s"))
    ops.head.schema = Some(schema)
    val plan = new DataflowPlan(ops)
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(plan.findOperatorForAlias("a").get))
    val expectedCode = cleanString("""
        |val a = b.map(t => PigFuncs.flatTuple(List(t(0),t(1))))""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for flattening a bag function in FOREACH" in {
    val ops = parseScript("b = load 'file'; a = foreach b generate flatten(tokenize($0));")
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType)))))
    ops.head.schema = Some(schema)
    val plan = new DataflowPlan(ops)
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(plan.findOperatorForAlias("a").get))
    val expectedCode = cleanString("""
        |val a = b.flatMap(t => PigFuncs.tokenize(t(0))).map(t => List(t))""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for flattening a bag in FOREACH" in {
    val ops = parseScript("b = load 'file'; a = foreach b generate $0, flatten($1);")
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
                                                              Field("f2", BagType(TupleType(
                                                                Array(Field("ff1", Types.IntType)), "s"), "b"))), "t"), "s"))
    ops.head.schema = Some(schema)
    val plan = new DataflowPlan(ops)
    val codeGenerator = new ScalaBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(plan.findOperatorForAlias("a").get))
    val expectedCode = cleanString("""
        |val a = b.flatMap(t => List(t(1).asInstanceOf[Seq[Any]].map(s => (List(t(0)), s))).map(t => List(t))""".stripMargin)
    assert(generatedCode == expectedCode)
  }
}
