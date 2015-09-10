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

package dbis.pipefabric.test

import dbis.pig.PigCompiler._
import dbis.pig.codegen.CppBackendGenCode
import dbis.pig.op._
import dbis.pig.plan.DataflowPlan
import dbis.pig.schema._
import org.scalatest.FlatSpec
import dbis.pig.backends.BackendManager

import java.net.URI
class PipefabricCompileSpec extends FlatSpec  {
  def cleanString(s: String): String = s.stripLineEnd.replaceAll("""\s+""", " ").trim

  val templateFile = BackendManager.backend("pipefabric").templateFile
  val file = new URI(new java.io.File(".").getCanonicalPath + "/file.csv")
  val threeFieldsSchema = Some(new Schema(BagType(TupleType(Array(
    Field("f1", Types.CharArrayType),
    Field("f2", Types.DoubleType),
    Field("f3", Types.IntType)), "t"), "s")))

  "The compiler output" should "contain the Pipefabric header & footer" in {
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitImport
      + codeGenerator.emitHeader1("test", "")
      + codeGenerator.emitHeader2("test")
      + codeGenerator.emitFooter)
    val expectedCode = cleanString("""
                                  |#include "pipefabric.hpp"
                                  |
                                  |using namespace pfabric;
                                  |
                                  |void buildQuery(QueryContext& qctx) {
                                  |   qctx.parameterizeOperators();
                                  |}
                                   """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for LOAD" in {
    val op = Load(Pipe("a"), file)
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(s"""
         |auto op_a_ = ns_types::make_shared<FileSource<a_TupleTypePtr_>>("${file}", ","); 
         |qctx.registerOperator("op_a", op_a_);
         |""".stripMargin)
    assert(generatedCode == expectedCode)
  }
  /*
  it should "contain code for LOAD with DBStorage" in {
    val op = Load(Pipe("a"), "SELECT col1, col2, col3 FROM MyTable", threeFieldsSchema, "DBStorage", List(new URI("sqlite3://test.db")))
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(s"""
         |auto prepareFun = [&](const soci::row& r) { 
         |  auto tp = makeTuplePtr(r.get<std::string>(0),r.get<double>(1),r.get<int>(2)); 
         |  return tp; 
         |}; 
         |auto op_a_ = ns_types::make_shared<DBSource<a_TupleTypePtr_>>("sqlite3://test.db", "SELECT col1, col2, col3 FROM MyTable", prepareFun); 
         |qctx.registerOperator("op_a", op_a_);
         |""".stripMargin)
    assert(generatedCode == expectedCode)
  }
  it should "contain code for LOAD with SPARQLStorage" in {
    val op = Load(Pipe("a"), "SELECT ?s ?p ?o WHERE { ?s ?p ?o }", None, "SPARQLStorage", List(NEW "redland:test.nt"))
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(s"""
         |auto op_a_ = ns_types::make_shared<SPARQLSource<a_TupleTypePtr_>>("redland","redland:test.nt", "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"); 
         |qctx.registerOperator("op_a", op_a_);
         |""".stripMargin)
    assert(generatedCode == expectedCode)
  }*/

  it should "contain code for FILTER with PositionalField" in {
    val op = Filter(Pipe("aa"), Pipe("bb"), Lt(RefExpr(PositionalField(0)), RefExpr(Value("42"))))
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
     auto op_aa_ = ns_types::make_shared<Filter<bb_TupleTypePtr_>>([&](const bb_TupleTypePtr_& tp) 
     |{ return ns_types::get<0>(*tp) < 42; }); 
     |makeLink(op_bb_, op_aa_); 
     |qctx.registerOperator("op_aa", op_aa_);
      """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for FILTER with NamedField" in {
    val op = Filter(Pipe("aa"), Pipe("bb"), Lt(RefExpr(NamedField("f2")), RefExpr(Value(42))))
    op.schema = Some(new Schema(BagType(TupleType(Array(
      Field("f1", Types.IntType),
      Field("f2", Types.CharArrayType)), "t"), "s")))
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
     auto op_aa_ = ns_types::make_shared<Filter<bb_TupleTypePtr_>>([&](const bb_TupleTypePtr_& tp) 
     |{ return ns_types::get<1>(*tp) < 42; }); 
     |makeLink(op_bb_, op_aa_); 
     |qctx.registerOperator("op_aa", op_aa_);
      """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "produce code for complex FILTER" in {
    val ops = parseScript("b = LOAD 'file' AS (x: double, y:double, z1:int, z2: int); c = FILTER b BY x > 0 AND (y < 0 OR (NOT z1 == z2));")
    val plan = new DataflowPlan(ops)
    val op = ops(1)
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      auto op_c_ = ns_types::make_shared<Filter<b_TupleTypePtr_>>([&](const b_TupleTypePtr_& tp) 
      |{ return ns_types::get<0>(*tp) > 0 && (ns_types::get<1>(*tp) < 0 || (!(ns_types::get<2>(*tp) == ns_types::get<3>(*tp)))); }); 
      |makeLink(op_b_, op_c_); 
      |qctx.registerOperator("op_c", op_c_);
      """.stripMargin)
    assert(generatedCode == expectedCode)

  }

  it should "contain code for DUMP" in {
    val op = Dump(Pipe("a"))
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
    auto op_dump_a_ = ns_types::make_shared<StreamWriter<a_TupleTypePtr_>>(std::cout); 
    |makeLink(op_a_, op_dump_a_); 
    |qctx.registerOperator("op_dump_a", op_dump_a_);
      """.stripMargin)
    assert(generatedCode == expectedCode)
  }
  /*
  it should "contain code for STORE" in {
    val op = Store(Pipe("A"), file)
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(s"""
      auto op_store_A_ = ns_types::make_shared<StreamWriter<A_TupleTypePtr_>>("/Users/omransaleh/git/pigspark/file.csv"); 
      |makeLink(op_A_, op_store_A_); 
      |qctx.registerOperator("op_store_A", op_store_A_);
      """.stripMargin)
    assert(generatedCode == expectedCode)
  }*/

  "The code generator" should "produce the correct tuple structure" in {
    val schema = Some(new Schema(BagType(TupleType(Array(
      Field("f1", Types.IntType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.CharArrayType),
      Field("f4", Types.IntType),
      Field("f5", Types.CharArrayType))))))
    val codeGenerator = new CppBackendGenCode(templateFile)
    val res = codeGenerator.schemaToTupleStruct(schema)
    assert(res == "int,double,std::string,int,std::string")
  }

  it should "contain code a foreach statement (only projection)" in {
    val ops = parseScript("b = LOAD 'file' AS (x: double, y:double, z1:int, z2: int); c = FOREACH b generate z1, z2;")
    val plan = new DataflowPlan(ops)
    val op = ops(1)
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
     auto op_c_ = ns_types::make_shared<Projection<b_TupleTypePtr_, c_TupleTypePtr_>>([&](const b_TupleTypePtr_& tp) {
     |  auto newTup = makeTuplePtr(ns_types::get<2>(*tp),ns_types::get<3>(*tp)); 
     |  return newTup; 
     |}); 
     |makeLink(op_b_, op_c_); qctx.registerOperator("op_c", op_c_);
     |""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code a foreach statement with calculations" in {
    val ops = parseScript("b = LOAD 'file' AS (x: double, y:double, z1:int, z2: int); c = FOREACH b generate z1/2 as cal1, z2/3 as cal2;")
    val plan = new DataflowPlan(ops)
    val op = ops(1)
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
     auto op_c_ = ns_types::make_shared<Projection<b_TupleTypePtr_, c_TupleTypePtr_>>([&](const b_TupleTypePtr_& tp) { 
     |  auto newTup = makeTuplePtr(ns_types::get<2>(*tp) / 2,ns_types::get<3>(*tp) / 3); 
     |  return newTup; 
     |
     |}); makeLink(op_b_, op_c_); 
     |qctx.registerOperator("op_c", op_c_);
     |""".stripMargin)
    assert(generatedCode == expectedCode)
  }
  /*
  it should "contain code for a foreach statement with a UDF expression" in {
    // c = FOREACH b GENERATE x, distance($1, $2, 1.0, 2.0) AS dist;
    val ops = parseScript("b = LOAD 'file' AS (x: double, y:double, z1:int, z2: int); c =  FOREACH b GENERATE x, distance($1, $2, 1.0, 2.0) AS dist;")
    val plan = new DataflowPlan(ops)
    val op = ops(1)
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      auto op_c_ = ns_types::make_shared<Projection<b_TupleTypePtr_, c_TupleTypePtr_>>([&](const b_TupleTypePtr_& tp) { 
      |  auto newTup = makeTuplePtr(ns_types::get<0>(*tp),); 
      |  return newTup; 
      |}); 
      |makeLink(op_b_, op_c_); 
      |qctx.registerOperator("op_c", op_c_);""".stripMargin)
    assert(generatedCode == expectedCode)
  }*/

  it should "contain code for a binary join statement with simple expression" in {
    
    val op = Join(Pipe("aa"), List(Pipe("bb"), Pipe("cc")), List(List(PositionalField(0)), List(PositionalField(0))))
    val input1 = Pipe("bb", Load(Pipe("bb"), file, threeFieldsSchema))
    val input2 = Pipe("cc", Load(Pipe("cc"), file, threeFieldsSchema))
    op.inputs = List(input1, input2)
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      auto op_aa_join_pred = [&](cc_TupleTypePtr_ tp1, bb_TupleTypePtr_ tp2) { 
      |  return ns_types::get<0>(*tp1) == ns_types::get<0>(*tp2); 
      |}; 
      |auto op_aa_lhash_fun = [&](cc_TupleTypePtr_ tp) { 
      |  std::size_t seed = 0; 
      |  boost::hash_combine(seed, ns_types::get<0>(*tp)); 
      |  return seed; 
      |}; 
      |auto op_aa_rhash_fun = [&](bb_TupleTypePtr_ tp) { 
      |  std::size_t seed = 0; 
      |  boost::hash_combine(seed, ns_types::get<0>(*tp)); 
      |  return seed; 
      |}; 
      |auto op_aa_ = ns_types::make_shared<RHJoin<cc_TupleTypePtr_, bb_TupleTypePtr_>>(op_aa_lhash_fun, op_aa_rhash_fun, op_aa_join_pred, false); 
      |makeLinks(op_cc_, op_bb_, op_aa_); 
      |qctx.registerOperator("op_aa", op_aa_);
      |""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a binary join statement with expression lists" in {
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c")), List(List(PositionalField(0), PositionalField(1)),
      List(PositionalField(1), PositionalField(2))))
    val input1 = Pipe("b", Load(Pipe("b"), file, threeFieldsSchema))
    val input2 = Pipe("c", Load(Pipe("c"), file, threeFieldsSchema))
    op.inputs = List(input1, input2)
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      auto op_a_join_pred = [&](c_TupleTypePtr_ tp1, b_TupleTypePtr_ tp2) { 
      |  return ns_types::get<0>(*tp1) == ns_types::get<1>(*tp2) && ns_types::get<1>(*tp1) == ns_types::get<2>(*tp2); 
      |}; 
      |auto op_a_lhash_fun = [&](c_TupleTypePtr_ tp) { 
      |  std::size_t seed = 0; 
      |  boost::hash_combine(seed, ns_types::get<1>(*tp)); 
      |  boost::hash_combine(seed, ns_types::get<2>(*tp)); 
      |  return seed; 
      |}; 
      |auto op_a_rhash_fun = [&](b_TupleTypePtr_ tp) { 
      |  std::size_t seed = 0; 
      |  boost::hash_combine(seed, ns_types::get<0>(*tp)); 
      |  boost::hash_combine(seed, ns_types::get<1>(*tp)); 
      |  return seed; 
      |}; 
      |auto op_a_ = ns_types::make_shared<RHJoin<c_TupleTypePtr_, b_TupleTypePtr_>>(op_a_lhash_fun, op_a_rhash_fun, op_a_join_pred, false); 
      |makeLinks(op_c_, op_b_, op_a_); 
      |qctx.registerOperator("op_a", op_a_);
      |""".stripMargin)
    assert(generatedCode == expectedCode)

  }

  it should "contain code for a multiway join statement" in {
    val op = Join(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")), List(List(PositionalField(0)),
      List(PositionalField(0)), List(PositionalField(0))))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)), "t"), "s"))
    val input1 = Pipe("b", Load(Pipe("b"), file, threeFieldsSchema))
    val input2 = Pipe("c", Load(Pipe("c"), file, threeFieldsSchema))
    val input3 = Pipe("d", Load(Pipe("d"), file, threeFieldsSchema))
    op.inputs = List(input1, input2, input3)
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      auto op_a_2_join_pred = [&](d_TupleTypePtr_ tp1, c_TupleTypePtr_ tp2) { 
      |  return ns_types::get<0>(*tp1) == ns_types::get<0>(*tp2); 
      |}; 
      |auto op_a_2_lhash_fun = [&](d_TupleTypePtr_ tp) { 
      |  std::size_t seed = 0; 
      |  boost::hash_combine(seed, ns_types::get<0>(*tp)); 
      |  return seed; }; 
      |auto op_a_2_rhash_fun = [&](c_TupleTypePtr_ tp) { 
      |  std::size_t seed = 0; 
      |  boost::hash_combine(seed, ns_types::get<0>(*tp)); 
      |  return seed; 
      |}; 
      |auto op_a_2_ = ns_types::make_shared<RHJoin<d_TupleTypePtr_, c_TupleTypePtr_>>(op_a_2_lhash_fun, op_a_2_rhash_fun, op_a_2_join_pred, false); 
      |makeLinks(op_d_, op_c_, op_a_2_); 
      |qctx.registerOperator("op_a_2", op_a_2_); 
      |auto op_a_join_pred = [&](a_2_TupleTypePtr_ tp1, b_TupleTypePtr_ tp2) { 
      |  return ns_types::get<0>(*tp1) == ns_types::get<0>(*tp2); 
      |}; auto op_a_lhash_fun = [&](a_2_TupleTypePtr_ tp) { 
      |  std::size_t seed = 0; 
      |  boost::hash_combine(seed, ns_types::get<0>(*tp)); 
      |  return seed; 
      |}; 
      |auto op_a_rhash_fun = [&](b_TupleTypePtr_ tp) { 
      |  std::size_t seed = 0; 
      |  boost::hash_combine(seed, ns_types::get<0>(*tp)); 
      |  return seed; 
      |}; 
      |auto op_a_ = ns_types::make_shared<RHJoin<a_2_TupleTypePtr_, b_TupleTypePtr_>>(op_a_lhash_fun, op_a_rhash_fun, op_a_join_pred, false); 
      |makeLinks(op_a_2_, op_b_, op_a_); 
      |qctx.registerOperator("op_a", op_a_);
      |""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a union operator on two relations" in {
    // a = UNION b, c;
    val op = Union(Pipe("aa"), List(Pipe("bb"), Pipe("cc")))
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
     auto op_aa_ = ns_types::make_shared<Merge<aa_TupleTypePtr_>>(); 
     |makeLinks(op_bb_, op_cc_, op_aa_); 
     |qctx.registerOperator("op_aa", op_aa_);
     """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for a union operator on more than two relations" in {
    // a = UNION b, c, d;
    val op = Union(Pipe("a"), List(Pipe("b"), Pipe("c"), Pipe("d")))
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
     auto op_a_2_ = ns_types::make_shared<Merge<a_TupleTypePtr_>>(); 
     |makeLinks(op_d_, op_c_, op_a_2_); 
     |qctx.registerOperator("op_a_2", op_a_2_); 
     |auto op_a_ = ns_types::make_shared<Merge<a_TupleTypePtr_>>(); 
     |makeLinks(op_b_, op_a_2_, op_a_);
     |qctx.registerOperator("op_a", op_a_);
     """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for Socket Read using ZMQ" in {
    val op = SocketRead(Pipe("a"), SocketAddress("tcp://", "localhost", "9999"), null)
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      auto op_a_ = ns_types::make_shared<ZMQSource<a_TupleTypePtr_>>("tcp://localhost:9999", ZMQParams::SubscriberSource, ZMQParams::BinaryMode); 
      |qctx.registerOperator("op_a", op_a_);
     """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for Socket Write using ZMQ" in {
    val op = SocketWrite(Pipe("a"), SocketAddress("tcp://", "localhost", "9999"), null)
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      auto op_zmq_sink_a_ = ns_types::make_shared<ZMQSink<a_TupleTypePtr_>>("tcp://localhost:9999", ZMQParams::PublisherSink, ZMQParams::BinaryMode); 
      |makeLink(a, op_zmq_sink_a_); 
      |qctx.registerOperator("op_zmq_sink_a", op_zmq_sink_a_);
    """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for simple ORDER BY" in {
    // aa = ORDER bb BY $0
    val op = OrderBy(Pipe("aa"), Pipe("bb"), List(OrderBySpec(PositionalField(0), OrderByDirection.AscendingOrder)))
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      auto op_aaComparePredicate = [&](const bb_TupleTypePtr_& tp, const bb_TupleTypePtr_& tp1) {
      |  return ( ns_types::get<0>(*tp)<ns_types::get<0>(*tp1));
      |}; 
      |auto op_aa_ = ns_types::make_shared<Sorter<bb_TupleTypePtr_>>(op_aaComparePredicate ); 
      |makeLink(op_bb_, op_aa_); 
      |qctx.registerOperator("op_aa", op_aa_);
      |""".stripMargin)

    assert(generatedCode == expectedCode)
  }

  it should "contain code for complex ORDER BY" in {
    // a = ORDER b BY f1, f3
    val op = OrderBy(Pipe("a"), Pipe("b"), List(OrderBySpec(NamedField("f1"), OrderByDirection.AscendingOrder),
      OrderBySpec(NamedField("f3"), OrderByDirection.AscendingOrder)))
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.CharArrayType),
      Field("f2", Types.DoubleType),
      Field("f3", Types.IntType)), "t"), "s"))

    op.schema = Some(schema)
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString("""
      auto op_aComparePredicate = [&](const b_TupleTypePtr_& tp, const b_TupleTypePtr_& tp1) { 
      |  return ( ns_types::get<0>(*tp)<ns_types::get<0>(*tp1)) || ( ns_types::get<2>(*tp)<ns_types::get<2>(*tp1) && ns_types::get<0>(*tp) == ns_types::get<0>(*tp1)); 
      |}; 
      |auto op_a_ = ns_types::make_shared<Sorter<b_TupleTypePtr_>>(op_aComparePredicate ); 
      |makeLink(op_b_, op_a_); 
      |qctx.registerOperator("op_a", op_a_);
      |""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for Sliding WINDOW operator using rows and slider " in {
    val op = Window(Pipe("a"), Pipe("b"), (10, ""), (10, "seconds"))
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(s"""
         |auto op_a_ = ns_types::make_shared<SlidingWindow<b_TupleTypePtr_>>(WindowParams::RowWindow, 10, 10); 
         |makeLink(op_b_, op_a_); 
         |qctx.registerOperator("op_a", op_a_);
         |""".stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain code for Sliding WINDOW operator using range and slider " in {
    val op = Window(Pipe("a"), Pipe("b"), (10, "minutes"), (10, "seconds"))
    val codeGenerator = new CppBackendGenCode(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(op))
    val expectedCode = cleanString(s"""
         |auto op_a_ = ns_types::make_shared<SlidingWindow<b_TupleTypePtr_>>(WindowParams::RangeWindow, 600, 10); 
         |makeLink(op_b_, op_a_); 
         |qctx.registerOperator("op_a", op_a_);
         |""".stripMargin)
    assert(generatedCode == expectedCode)
  }
  // it should throw an exception since  row sliding is not supported
  it should "throw  an exception for Sliding WINDOW operator using row and row slider " in {
    val op = Window(Pipe("a"), Pipe("b"), (10, "minutes"), (10, ""))
    val codeGenerator = new CppBackendGenCode(templateFile)
    val thrown = intercept[Exception] {
      val generatedCode = cleanString(codeGenerator.emitNode(op))
    }
    assert(thrown.getMessage === "Currently, row sliding is not supported")
  }
  
  // More works on aggregation
}