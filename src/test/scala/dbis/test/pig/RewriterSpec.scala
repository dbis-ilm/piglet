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
package dbis.test.pig

import java.net.URI

import dbis.pig.PigCompiler._
import dbis.pig.op._
import dbis.pig.parser.{LanguageFeature, PigParser}
import dbis.pig.plan.rewriting.Extractors.{AllSuccE, ForEachCallingFunctionE, SuccE}
import dbis.pig.plan.rewriting.Rewriter._
import dbis.pig.plan.rewriting.Rules._
import dbis.pig.plan.rewriting.rulesets.GeneralRuleset._
import dbis.pig.plan.rewriting.rulesets.RDFRuleset._
import dbis.pig.plan.rewriting.{Functions, Rewriter}
import dbis.pig.plan.{DataflowPlan, PipeNameGenerator}
import dbis.pig.schema.{BagType, Schema, TupleType, _}
import dbis.test.TestTools._
import org.kiama.rewriting.Rewriter.strategyf
import org.scalatest.OptionValues._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers, PrivateMethodTester}

import scala.util.Random

class RewriterSpec extends FlatSpec
                   with Matchers
                   with TableDrivenPropertyChecks
                   with BeforeAndAfterEach
                   with PrivateMethodTester {
  override def beforeEach(): Unit = {
    val resetMethod = PrivateMethod[Unit]('resetStrategy)
    Rewriter invokePrivate resetMethod()
  }

  private def performReorderingTest() = {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val predicate1 = Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))

    // ops before reordering
    val op2 = OrderBy(Pipe("b"), Pipe("a"), List())
    val op3 = Filter(Pipe("c"), Pipe("b"), predicate1)
    val op4 = Dump(Pipe("c"))

    val plan = new DataflowPlan(List(op1, op2, op3, op4))
    val pPlan = processPlan(plan)
    val rewrittenSource = pPlan.sourceNodes.headOption.value

    rewrittenSource.outputs should contain only Pipe("a", rewrittenSource, List(op3))
    pPlan.findOperatorForAlias("b").value shouldBe op3
    pPlan.sinkNodes.headOption.value shouldBe op4
    pPlan.sinkNodes.headOption.value.inputs.headOption.value.producer shouldBe op2
    op2.outputs.flatMap(_.consumer) should contain only op4
    op4.inputs.map(_.producer) should contain only op2
  }

  private def performMergeTest() = {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val predicate1 = Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))
    val predicate2 = Neq(RefExpr(PositionalField(1)), RefExpr(Value("21")))
    val op2 = Filter(Pipe("b"), Pipe("a"), predicate1)
    val op3 = Filter(Pipe("c"), Pipe("b"), predicate2)
    val op4 = Dump(Pipe("c"))
    val op4_2 = op4.copy()
    val opMerged = Filter(Pipe("c"), Pipe("a"), And(predicate1, predicate2))

    val planUnmerged = new DataflowPlan(List(op1, op2, op3, op4))
    val planMerged = new DataflowPlan(List(op1, opMerged, op4_2))
    val source = planUnmerged.sourceNodes.head
    val sourceMerged = planMerged.sourceNodes.head

    val rewrittenSink = processPigOperator(source)
    rewrittenSink.asInstanceOf[PigOperator].outputs should equal(sourceMerged.outputs)

    val pPlan = processPlan(planUnmerged)
    pPlan.findOperatorForAlias("c").value should be(opMerged)
    pPlan.findOperatorForAlias("a").value.outputs.head.consumer should contain only opMerged
  }

  private def performRemovalTest() = {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val predicate1 = Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))

    // ops before removing
    val op2 = OrderBy(Pipe("b"), Pipe("a"), List())
    val op3 = Filter(Pipe("c"), Pipe("b"), predicate1)
    val op4 = Dump(Pipe("c"))

    val plan = new DataflowPlan(List(op1, op2, op3, op4))
    val pPlan = processPlan(plan)
    val rewrittenSource = pPlan.sourceNodes.headOption.value

    rewrittenSource.outputs should contain only Pipe("a", rewrittenSource, List(op3))
    pPlan.findOperatorForAlias("b") shouldBe empty
    pPlan.sinkNodes.headOption.value shouldBe op4
    pPlan.sinkNodes.headOption.value.inputs.headOption.value.producer shouldBe op3
    op4.inputs.map(_.producer) should contain only op3
  }
  "The rewriter" should "merge two Filter operations" in {
    merge[Filter, Filter](mergeFilters)
    performMergeTest()
  }

  it should "remove Filter operation if it has the same predicate as an earlier one" in {
    addStrategy(removeDuplicateFilters)
    val op1 = Load(Pipe("a"), "input/file.csv")
    val predicate1 = Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))
    val predicate2 = Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))
    val op2 = Filter(Pipe("b"), Pipe("a"), predicate1)
    val op2_after = Filter(Pipe("c"), Pipe("a"), predicate1)
    val op3 = Filter(Pipe("c"), Pipe("b"), predicate2)
    val op4 = Dump(Pipe("c"))

    val plan = new DataflowPlan(List(op1, op2, op3, op4))

    val pPlan = processPlan(plan)
    pPlan.findOperatorForAlias("b").value should be(op2)
    pPlan.findOperatorForAlias("a").value.outputs.head.consumer should contain only op2
    op2.outputs.flatMap(_.consumer) should contain only op4
    op4.inputs.map(_.producer) should contain only op2
  }

  it should "order Filter operations before Order By ones" in {
    reorder[OrderBy, Filter]
    performReorderingTest()
  }

  it should "order Filter operations before Joins if only NamedFields are used" in {
    addStrategy(buildBinaryPigOperatorStrategy[Join, Filter](filterBeforeMultipleInputOp))
    val load_1 = Load(Pipe("a"), "input/file.csv", Some(Schema(BagType(TupleType(Array(Field("a", Types.IntType), Field("aid", Types.IntType)))
    ))))
    val load_2 = Load(Pipe("b"), "file2.csv", Some(Schema(BagType(TupleType(Array(Field("b", Types.CharArrayType), Field
      ("bid", Types.IntType)))
    ))))
    val predicate1 = Lt(RefExpr(NamedField("a")), RefExpr(Value("42")))

    // ops before reordering
    val join = Join(Pipe("c"), List(Pipe("a"), Pipe("b")),
      List(List(NamedField("aid")),
           List(NamedField("bid"))
      ))
    val filter = Filter(Pipe("d"), Pipe("c"), predicate1)
    val dump = Dump(Pipe("d"))

    val plan = processPlan(new DataflowPlan(List(load_1, load_2, join, filter, dump)))
    load_1.outputs.headOption.value.consumer should contain only filter
    load_2.outputs.headOption.value.consumer should contain only join
    filter.outputs.headOption.value.consumer should contain only join
    join.outputs.headOption.value.consumer should contain only dump
    filter.schema shouldBe load_1.schema

    load_1.outputs should have length 1
    load_2.outputs should have length 1
    join.outputs should have length 1
    filter.outputs should have length 1

    plan.findOperatorForAlias("c").headOption.value.inputs.map(_.producer) should contain only(filter, load_2)
  }

  it should "order Filter operations before Cross operators if only NamedFields are used" in {
    addStrategy(buildBinaryPigOperatorStrategy[Cross, Filter](filterBeforeMultipleInputOp))
    val load_1 = Load(Pipe("a"), "input/file.csv", Some(Schema(BagType(TupleType(Array(Field("a", Types.IntType), Field("aid", Types.IntType)))
    ))))
    val load_2 = Load(Pipe("b"), "file2.csv", Some(Schema(BagType(TupleType(Array(Field("b", Types.CharArrayType), Field
      ("bid", Types.IntType)))
    ))))
    val predicate1 = Lt(RefExpr(NamedField("a")), RefExpr(Value("42")))

    // ops before reordering
    val cross = Cross(Pipe("c"), List(Pipe("a"), Pipe("b")))
    val filter = Filter(Pipe("d"), Pipe("c"), predicate1)
    val dump = Dump(Pipe("d"))

    val plan = processPlan(new DataflowPlan(List(load_1, load_2, cross, filter, dump)))
    load_1.outputs.headOption.value.consumer should contain only filter
    load_2.outputs.headOption.value.consumer should contain only cross
    filter.outputs.headOption.value.consumer should contain only cross
    cross.outputs.headOption.value.consumer should contain only dump
    filter.schema shouldBe load_1.schema

    load_1.outputs should have length 1
    load_2.outputs should have length 1
    cross.outputs should have length 1
    filter.outputs should have length 1

    plan.findOperatorForAlias("c").headOption.value.inputs.map(_.producer) should contain only(filter, load_2)
  }

  it should "rewrite DataflowPlans without introducing read-before-write conflicts" in {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val predicate = Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))
    val op2 = Filter(Pipe("b"), Pipe("a"), predicate)
    val op3 = Dump(Pipe("b"))
    val op4 = OrderBy(Pipe("c"), Pipe("b"), List())
    val op5 = Dump(Pipe("c"))
    val plan = new DataflowPlan(List(op1, op2, op3, op4, op5))

    val newPlan = processPlan(plan)
    // Check that for each operator all operators in its input list are sorted before it in the operator list
    for (op <- newPlan.operators) {
      val currentIndex = newPlan.operators.indexOf(op)
      for (input <- op.inputs.map(_.producer)) {
        val inputIndex = newPlan.operators.indexOf(input)
        withClue(op.toString ++ input.toString) {
          assert(currentIndex > inputIndex)
        }
      }
    }
  }

  it should "not reorder operators if the first one has more than one output" in {
    registerAllRules()
    val op1 = Load(Pipe("a"), "input/file.csv")
    val predicate1 = Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))

    // ops before reordering
    val op2 = OrderBy(Pipe("b"), Pipe("a"), List())
    val op3 = Filter(Pipe("c"), Pipe("b"), predicate1)
    val op4 = Dump(Pipe("c"))
    val op5 = Dump(Pipe("b"))

    val plan = new DataflowPlan(List(op1, op2, op3, op4, op5))
    val source = plan.sourceNodes.head

    val rewrittenSource = processPigOperator(source)
    rewrittenSource.asInstanceOf[PigOperator].outputs should contain only Pipe("a", op1, List(op2))
  }

  it should "remove sink nodes that don't store a relation" in {
    addStrategy(removeNonStorageSinks _)
    val op1 = Load(Pipe("a"), "input/file.csv")
    val plan = new DataflowPlan(List(op1))
    val newPlan = processPlan(plan)

    newPlan.sourceNodes shouldBe empty
  }

  it should "remove sink nodes that don't store a relation and have an empty outputs list" in {
    addStrategy(removeNonStorageSinks _)
    val op1 = Load(Pipe("a"), "input/file.csv")
    val plan = new DataflowPlan(List(op1))

    op1.outputs = List.empty
    val newPlan = processPlan(plan)

    newPlan.sourceNodes shouldBe empty
  }

  it should "pull up Empty nodes" in {
    addStrategy(removeNonStorageSinks _)
    merge[PigOperator, Empty](mergeWithEmpty)
    val op1 = Load(Pipe("a"), "input/file.csv")
    val op2 = OrderBy(Pipe("b"), Pipe("a"), List())
    val plan = new DataflowPlan(List(op1, op2))
    plan.operators should have length 2

    val newPlan = processPlan(plan)

    newPlan.sourceNodes shouldBe empty
    newPlan.operators shouldBe empty
  }

  it should "apply rewriting rule R1" in {
    Rewriter toReplace (classOf[RDFLoad]) applyRule R1
    val URLs = Table(
      ("url"),
      ("http://www.example.com"),
      ("https://www.example.com")
    )
    forAll(URLs) { (url: String) =>
      val op1 = RDFLoad(Pipe("a"), new URI(url), None)
      val op2 = Dump(Pipe("a"))
      val plan = processPlan(new DataflowPlan(List(op1, op2)))
      val source = plan.sourceNodes.headOption.value
      source shouldBe Load(Pipe("a"), url, op1.schema, Some("pig.SPARQLLoader"),
        List("SELECT * WHERE { ?s ?p ?o }"))
    }
  }

  it should "apply rewriting rule R2" in {
    Rewriter applyRule R2
    val op1 = RDFLoad(Pipe("a"), new URI("http://example.com"), None)
    val op2 = OrderBy(Pipe("b"), Pipe("a"), List(OrderBySpec(NamedField("subject"), OrderByDirection.DescendingOrder)))
    val op3 = BGPFilter(Pipe("c"), Pipe("b"),
      List(
        TriplePattern(
          PositionalField(0),
          Value(""""firstName""""),
          Value(""""Stefan""""))))
    val op4 = Dump(Pipe("c"))
    val plan = processPlan(new DataflowPlan(List(op1, op2, op3, op4)))
    val source = plan.sourceNodes.headOption.value
    source shouldBe Load(Pipe("a"), "http://example.com", op1.schema, Some("pig.SPARQLLoader"),
      List("""CONSTRUCT * WHERE { $0 "firstName" "Stefan" }"""))
    plan.operators should not contain op3
  }

  it should "not apply rewriting rule R2 if the schema of the next BGPFilter does not match the RDFLoads schema" in {
    Rewriter applyRule R1
    Rewriter applyRule R2
    val op1 = RDFLoad(Pipe("a"), new URI("http://example.com"), None)
    val op2 = OrderBy(Pipe("b"), Pipe("a"), List(OrderBySpec(NamedField("subject"), OrderByDirection.DescendingOrder)))
    val op3 = BGPFilter(Pipe("c"), Pipe("b"),
      List(
        TriplePattern(
          PositionalField(0),
          Value(""""firstName""""),
          Value(""""Wieland"""")),
        TriplePattern(
          PositionalField(0),
          Value(""""lastName""""),
          Value(""""Hoffmann""""))))
    val op4 = Dump(Pipe("c"))
    val plan = processPlan(new DataflowPlan(List(op1, op2, op3, op4)))
    val source = plan.sourceNodes.headOption.value
    source shouldBe Load(Pipe("a"), "http://example.com", op1.schema, Some("pig.SPARQLLoader"),
      List("""SELECT * WHERE { ?s ?p ?o }"""))
  }

  it should "apply rewriting rule L2" in {
    Rewriter toReplace (classOf[RDFLoad]) applyRule L2
    val possibleGroupers = Table(("grouping column"), ("subject"), ("predicate"), ("object"))
    forAll (possibleGroupers) { (g: String) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = Dump(Pipe("a"))
      val plan = processPlan(new DataflowPlan(List(op1, op2)))
      val source = plan.sourceNodes.headOption.value
      source shouldBe Load(Pipe("a"), "hdfs://somewhere", op1.schema, Some("BinStorage"))
    }
  }

  it should "apply rewriting rule F1" in {
    Rewriter applyRule F1
    val op1 = RDFLoad(Pipe("a"), new URI("http://example.com"), None)
    // Add something between op1 and op3 to prevent R2 from being applied
    val op2 = Distinct(Pipe("b"), Pipe("a"))
    val op3 = BGPFilter(Pipe("c"), Pipe("b"),
      List(
        TriplePattern(
          PositionalField(0),
          NamedField("predicate"),
          PositionalField(2))))
    val op4 = Dump(Pipe("c"))
    val plan = processPlan(new DataflowPlan(List(op1, op2, op3, op4)))
    val source = plan.sourceNodes.headOption.value
    source.outputs.flatMap(_.consumer) should contain only(op2)

    val sink = plan.sinkNodes.headOption.value
    sink shouldBe op4
    sink.inputs.map(_.producer) should contain only(op2)
  }

  it should "apply rewriting rule F2" in {
    Rewriter toReplace (classOf[BGPFilter]) applyRule F2
    val patterns = Table(
      ("Pattern"),
      (TriplePattern(Value("subjectv"), PositionalField(1), PositionalField(2)),
        Filter(Pipe("b"), Pipe("a"), Eq(RefExpr(NamedField("subject")), RefExpr(Value("subjectv"))))),
      (TriplePattern(PositionalField(0), Value("predicatev"), PositionalField(2)),
      Filter(Pipe("b"), Pipe("a"), Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicatev"))))),
      (TriplePattern(PositionalField(0), PositionalField(1), Value("objectv")),
      Filter(Pipe("b"), Pipe("a"), Eq(RefExpr(NamedField("object")), RefExpr(Value("objectv"))))))
    forAll (patterns) { (p: (TriplePattern, Filter)) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p._1))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only p._2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only p._2
    }

    val possibleGroupers = Table(("grouping column"), ("subject"), ("predicate"), ("object"))

    forAll (possibleGroupers) { (g: String) =>
      forAll(patterns) { (p: (TriplePattern, Filter)) =>
        val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
        val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p._1))
        val op3 = Dump(Pipe("b"))
        val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))

        plan.findOperatorForAlias("b").value.outputs.flatMap(_.consumer) should contain only op3
      }
    }
  }

  it should "apply rewriting rule F3" in {
    Rewriter toReplace (classOf[BGPFilter]) applyRule F3
    val patterns = Table(
      ("Pattern"),
      // s p o bound
      (TriplePattern(Value("subjectv"), Value("predicatev"), Value("objectv")),
        Filter(Pipe("b"), Pipe("a"), And(
          Eq(RefExpr(NamedField("subject")), RefExpr(Value("subjectv"))),
          And(
            Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicatev"))),
            Eq(RefExpr(NamedField("object")), RefExpr(Value("objectv"))))))),
      // s p bound
      (TriplePattern(Value("subjectv"), Value("predicatev"), PositionalField(2)),
        Filter(Pipe("b"), Pipe("a"), And(
          Eq(RefExpr(NamedField("subject")), RefExpr(Value("subjectv"))),
          Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicatev")))))),
      // s o bound
      (TriplePattern(Value("subjectv"), PositionalField(1), Value("objectv")),
        Filter(Pipe("b"), Pipe("a"), And(
          Eq(RefExpr(NamedField("subject")), RefExpr(Value("subjectv"))),
          Eq(RefExpr(NamedField("object")), RefExpr(Value("objectv")))))),
      // p o bound
      (TriplePattern(PositionalField(0), Value("predicatev"), Value("objectv")),
        Filter(Pipe("b"), Pipe("a"), And(
          Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicatev"))),
          Eq(RefExpr(NamedField("object")), RefExpr(Value("objectv")))))))

    forAll (patterns) { (p: (TriplePattern, Filter)) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p._1))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only p._2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only p._2
    }

    val possibleGroupers = Table(("grouping column"), ("subject"), ("predicate"), ("object"))

    // Apply F3 only to plain triples
    forAll (possibleGroupers) { (g: String) =>
      forAll(patterns) { (p: (TriplePattern, Filter)) =>
        val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
        val op2 = Distinct(Pipe("b"), Pipe("a"))
        val op3 = BGPFilter(Pipe("c"), Pipe("b"), List(p._1))
        val op4 = Dump(Pipe("c"))
        val plan = processPlan(new DataflowPlan(List(op1, op2, op3, op4)))

        plan.findOperatorForAlias("b").value.outputs.flatMap(_.consumer) should contain only op3
      }
    }
  }

  it should "apply rewriting rule F4" in {
    Rewriter toReplace (classOf[BGPFilter]) applyRule F4
    val patterns = Table(
      ("Pattern", "grouping column", "Filter"),
      (TriplePattern(Value("subject"), PositionalField(1), PositionalField(2)),
        "subject",
        Filter(Pipe("b"), Pipe("a"), Eq(RefExpr(NamedField("subject")), RefExpr(Value("subject"))))),
      (TriplePattern(PositionalField(0), Value("predicate"), PositionalField(2)),
        "predicate",
        Filter(Pipe("b"), Pipe("a"), Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicate"))))),
      (TriplePattern(PositionalField(0), PositionalField(1), Value("object")),
        "object",
        Filter(Pipe("b"), Pipe("a"), Eq(RefExpr(NamedField("object")), RefExpr(Value("object"))))))

    forAll (patterns) { (p: TriplePattern, g: String, f: Filter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only f
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only f
    }

    val possibleGroupers = Table(("grouping column"), ("subject"), ("predicate"), ("object"))

    // Test that F4 is only applied if the BGP filters by the grouping column
    forAll (possibleGroupers) { (g: String) =>
      forAll(patterns) { (p: TriplePattern, grouped_by: String, f: Filter) =>
        whenever(g != grouped_by) {
          val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
          val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
          val op3 = Dump(Pipe("b"))
          val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))

          plan.findOperatorForAlias("b").value.outputs.flatMap(_.consumer) should contain only op3
          plan.operators should contain only(op1, op2, op3)
        }
      }
    }
  }

  it should "apply rewriting rule F5" in {
    Rewriter applyRule F5
    val patterns = Table(
      ("Pattern", "grouping column", "ForEach", "Filter"),
      (TriplePattern(Value("subject"), PositionalField(1), PositionalField(2)),
        "subject",
        Foreach(Pipe("pipePClecYbNXF"), Pipe("a"), GeneratorPlan(List(
          Filter(Pipe("pipevHyYGvOfsZ"), Pipe("stmts_1"), Eq(RefExpr(NamedField("subject")), RefExpr(Value("subject")
          ))),
          Generate(
            List(
              GeneratorExpr(RefExpr(NamedField("*"))),
              GeneratorExpr(Func("COUNT",
                                List(RefExpr(NamedField("pipevHyYGvOfsZ")))),
                                Some(Field("cnt", Types.ByteArrayType)))))))),
        Filter(Pipe("b"), Pipe("pipePClecYbNXF"), Gt(RefExpr(NamedField("cnt")), RefExpr(Value(0))))),
      (TriplePattern(PositionalField(0), Value("predicate"), PositionalField(2)),
        "predicate",
        Foreach(Pipe("pipePClecYbNXF"), Pipe("a"), GeneratorPlan(List(
          Filter(Pipe("pipevHyYGvOfsZ"), Pipe("stmts_1"), Eq(RefExpr(NamedField("predicate")), RefExpr(Value
            ("predicate")))),
          Generate(
            List(
              GeneratorExpr(RefExpr(NamedField("*"))),
              GeneratorExpr(Func("COUNT",
                List(RefExpr(NamedField("pipevHyYGvOfsZ")))),
                Some(Field("cnt", Types.ByteArrayType)))))))),
        Filter(Pipe("b"), Pipe("pipePClecYbNXF"), Gt(RefExpr(NamedField("cnt")), RefExpr(Value(0))))),
      (TriplePattern(PositionalField(0), PositionalField(1), Value("object")),
        "object",
        Foreach(Pipe("pipePClecYbNXF"), Pipe("a"), GeneratorPlan(List(
          Filter(Pipe("pipevHyYGvOfsZ"), Pipe("stmts_1"), Eq(RefExpr(NamedField("object")), RefExpr(Value("object")))),
          Generate(
            List(
              GeneratorExpr(RefExpr(NamedField("*"))),
              GeneratorExpr(Func("COUNT",
                List(RefExpr(NamedField("pipevHyYGvOfsZ")))),
                Some(Field("cnt", Types.ByteArrayType)))))))),
        Filter(Pipe("b"), Pipe("pipePClecYbNXF"), Gt(RefExpr(NamedField("cnt")), RefExpr(Value(0))))))

    val possibleGroupers = Table(("grouping column"), ("subject"), ("predicate"), ("object"))

    forAll (possibleGroupers) { (g: String) =>
      forAll(patterns) { (p: TriplePattern, grouped_by: String, fo: Foreach, fi: Filter) =>
        // We generate multiple pipe names for each `p`, therefore we need to reset the random generators state not
        // at the beginning of this function, but before processing each combination of pattern and grouping column.
        Random.setSeed(123456789)
        PipeNameGenerator.clearGenerated
        // Test that F5 is only applied if the BGP does not filter by the grouping column
        whenever(g != grouped_by) {
          val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
          val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
          val op3 = Dump(Pipe("b"))
          val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))

          plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only fo
          plan.findOperatorForAlias("b").value.outputs.flatMap(_.consumer) should contain only op3
          plan.findOperatorForAlias("b").value shouldBe fi
          plan.operators should contain only(op1, fo, fi, op3)
        }

        // If the filter uses the grouping column, nothing should happen
         whenever(g == grouped_by) {
           val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
           val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
           val op3 = Dump(Pipe("b"))
           val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))

           plan.findOperatorForAlias("b").value.outputs.flatMap(_.consumer) should contain only op3
           plan.operators should contain only(op1, op2, op3)
         }
      }
    }
  }

  it should "apply rewriting rule F6" in {
    Rewriter applyRule F6
    val patterns = Table(
      ("Pattern", "bound columns", "ForEach", "Filter"),
      (TriplePattern(Value("subject"), Value("predicate"), PositionalField(2)),
        List("subject", "predicate"),
        Foreach(Pipe("pipePClecYbNXF"), Pipe("a"), GeneratorPlan(List(
          Filter(Pipe("pipevHyYGvOfsZ"), Pipe("stmts_1"),
            And(
              Eq(RefExpr(NamedField("subject")), RefExpr(Value("subject"))),
              Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicate"))))),
          Generate(
            List(
              GeneratorExpr(RefExpr(NamedField("*"))),
              GeneratorExpr(Func("COUNT",
                List(RefExpr(NamedField("pipevHyYGvOfsZ")))),
                Some(Field("cnt", Types.ByteArrayType)))))))),
        Filter(Pipe("b"), Pipe("pipePClecYbNXF"), Gt(RefExpr(NamedField("cnt")), RefExpr(Value(0))))),
      (TriplePattern(PositionalField(0), Value("predicate"), Value("object")),
        List("predicate", "object"),
        Foreach(Pipe("pipePClecYbNXF"), Pipe("a"), GeneratorPlan(List(
          Filter(Pipe("pipevHyYGvOfsZ"), Pipe("stmts_1"),
            And(
              Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicate"))),
              Eq(RefExpr(NamedField("object")), RefExpr(Value("object"))))),
          Generate(
            List(
              GeneratorExpr(RefExpr(NamedField("*"))),
              GeneratorExpr(Func("COUNT",
                List(RefExpr(NamedField("pipevHyYGvOfsZ")))),
                Some(Field("cnt", Types.ByteArrayType)))))))),
        Filter(Pipe("b"), Pipe("pipePClecYbNXF"), Gt(RefExpr(NamedField("cnt")), RefExpr(Value(0))))),
      (TriplePattern(Value("subject"), PositionalField(1), Value("object")),
        List("subject", "object"),
        Foreach(Pipe("pipePClecYbNXF"), Pipe("a"), GeneratorPlan(List(
          Filter(Pipe("pipevHyYGvOfsZ"), Pipe("stmts_1"),
            And(
              Eq(RefExpr(NamedField("subject")), RefExpr(Value("subject"))),
              Eq(RefExpr(NamedField("object")), RefExpr(Value("object"))))),
          Generate(
            List(
              GeneratorExpr(RefExpr(NamedField("*"))),
              GeneratorExpr(Func("COUNT",
                List(RefExpr(NamedField("pipevHyYGvOfsZ")))),
                Some(Field("cnt", Types.ByteArrayType)))))))),
        Filter(Pipe("b"), Pipe("pipePClecYbNXF"), Gt(RefExpr(NamedField("cnt")), RefExpr(Value(0))))))

    val possibleGroupers = Table(("grouping column"), ("subject"), ("predicate"), ("object"))

    forAll (possibleGroupers) { (g: String) =>
      forAll(patterns) { (p: TriplePattern, bound_columns: List[String], fo: Foreach, fi: Filter) =>
        // We generate multiple pipe names for each `p`, therefore we need to reset the random generators state not
        // at the beginning of this function, but before processing each combination of pattern and grouping column.
        Random.setSeed(123456789)
        PipeNameGenerator.clearGenerated
        // Test that F5 is only applied if the BGP does not filter by the grouping column
        whenever(!(bound_columns contains g)) {
          val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
          val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
          val op3 = Dump(Pipe("b"))
          val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))

          plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only fo
          plan.findOperatorForAlias("b").value.outputs.flatMap(_.consumer) should contain only op3
          plan.findOperatorForAlias("b").value shouldBe fi
          plan.operators should contain only(op1, fo, fi, op3)
        }

        // If the filter uses the grouping column, nothing should happen
        whenever(bound_columns contains g) {
          val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
          val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
          val op3 = Dump(Pipe("b"))
          val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))

          plan.findOperatorForAlias("b").value.outputs.flatMap(_.consumer) should contain only op3
          plan.operators should contain only(op1, op2, op3)
        }
      }
    }
  }

  it should "apply rewriting rule F7" in {
    Rewriter applyRule F7
    // F7 needs to build an internal pipe between the two new operators, its name is determined via Random.nextString
    // (10). The tests below include the values that are generated by Random.nextString(10) after setting the seed to
    // 123456789.
    Random.setSeed(123456789)
    PipeNameGenerator.clearGenerated
    val pattern = TriplePattern(Value("subject"),Value("predicate"), Value("object"))
    val patterns = Table(
      ("Pattern", "grouping column", "Grouping column Filter", "Other Filter"),
      // subject & predicate bound, grouped by subject
      (TriplePattern(Value("subject"), Value("predicate"), PositionalField(2)),
        "subject",
        BGPFilter(Pipe("pipePClecYbNXF"), Pipe("a"), List(TriplePattern(Value("subject"), PositionalField(1),
          PositionalField(2)))),
        BGPFilter(Pipe("b"), Pipe("pipePClecYbNXF"),
          List(TriplePattern(PositionalField(0), Value("predicate"), PositionalField(2))))),
      // subject & predicate bound, grouped by predicate
      (TriplePattern(Value("subject"), Value("predicate"), PositionalField(2)),
        "predicate",
        BGPFilter(Pipe("pipevHyYGvOfsZ"), Pipe("a"), List(TriplePattern(PositionalField(0), Value("predicate"),
          PositionalField(2)))),
        BGPFilter(Pipe("b"), Pipe("pipevHyYGvOfsZ"),
          List(TriplePattern(Value("subject"), PositionalField(1), PositionalField(2))))),
      // subject & object bound, grouped by subject
      (TriplePattern(Value("subject"), PositionalField(1), Value("object")),
        "subject",
        BGPFilter(Pipe("pipeEgkYzrkOZO"), Pipe("a"), List(TriplePattern(Value("subject"), PositionalField(1),
          PositionalField(2)))),
        BGPFilter(Pipe("b"), Pipe("pipeEgkYzrkOZO"),
          List(TriplePattern(PositionalField(0), PositionalField(1), Value("object"))))),
      // subject & object bound, grouped by object
      (TriplePattern(Value("subject"), PositionalField(1), Value("object")),
        "object",
        BGPFilter(Pipe("pipejvdcHuREqz"), Pipe("a"), List(TriplePattern(PositionalField(0), PositionalField(1), Value
          ("object")))),
        BGPFilter(Pipe("b"), Pipe("pipejvdcHuREqz"),
          List(TriplePattern(Value("subject"), PositionalField(1), PositionalField(2))))),
      // predicate & object bound, grouped by predicate
      (TriplePattern(PositionalField(0), Value("predicate"), Value("object")),
        "predicate",
        BGPFilter(Pipe("pipeUxwEkfQHGx"), Pipe("a"), List(TriplePattern(PositionalField(0), Value("predicate"),
          PositionalField(2)))),
        BGPFilter(Pipe("b"), Pipe("pipeUxwEkfQHGx"),
          List(TriplePattern(PositionalField(0), PositionalField(1), Value("object"))))),
      // predicate & object bound, grouped by object
      (TriplePattern(PositionalField(0), Value("predicate"), Value("object")),
        "object",
        BGPFilter(Pipe("pipeYAXKzBIYXu"), Pipe("a"),
          List(TriplePattern(PositionalField(0), PositionalField(1), Value("object")))),
        BGPFilter(Pipe("b"), Pipe("pipeYAXKzBIYXu"),
          List(TriplePattern(PositionalField(0), Value("predicate"), PositionalField(2)))))
    )

    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only f1
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only f2
    }

    // Don't apply F7 to non-grouped data
    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }

    // Don't apply F7 if there's more than one pattern
    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p, p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }

    // Don't apply F7 if there are no patterns
    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List.empty)
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }

    val possibleGroupers = Table(("grouping column"), ("subject"), ("predicate"), ("object"))

    // Test that F7 is not applied if the pattern doesn't filter by the grouping column
    forAll (possibleGroupers) { (g: String) =>
      forAll(patterns) { (p: TriplePattern, grouped_by: String, f1: BGPFilter, f2: BGPFilter) =>
        whenever(g != grouped_by &&
          // These are all the cases where the column that's grouped by is not bound in the pattern
          !(g == "subject" &&  p.subj.isInstanceOf[Value]
            || g == "predicate" && p.pred.isInstanceOf[Value]
            || g == "object" && p.obj.isInstanceOf[Value])) {
          val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
          val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
          val op3 = Dump(Pipe("b"))
          val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))

          plan.findOperatorForAlias("b").value.outputs.flatMap(_.consumer) should contain only op3
          plan.operators should contain only(op1, op2, op3)
        }
      }
    }
  }

  it should "apply rewriting rule F8" in {
    Rewriter applyRule F8
    // F8 needs to build an internal pipe between the two new operators, its name is determined via Random.nextString
    // (10). The tests below include the values that are generated by Random.nextString(10) after setting the seed to
    // 123456789.
    Random.setSeed(123456789)
    PipeNameGenerator.clearGenerated
    val pattern = TriplePattern(Value("subject"),Value("predicate"), Value("object"))
    val patterns = Table(
      ("Pattern", "grouping column", "Grouping column Filter", "Other Filter"),
      (pattern,
        "subject",
        BGPFilter(Pipe("pipePClecYbNXF"), Pipe("a"), List(TriplePattern(Value("subject"), PositionalField(1),
          PositionalField(2)))),
        BGPFilter(Pipe("b"), Pipe("pipePClecYbNXF"), List(TriplePattern(PositionalField(0), Value("predicate"),
          Value("object"))))),
      (pattern,
        "predicate",
        BGPFilter(Pipe("pipevHyYGvOfsZ"), Pipe("a"), List(TriplePattern(PositionalField(0), Value("predicate"),
          PositionalField(2)))),
        BGPFilter(Pipe("b"), Pipe("pipevHyYGvOfsZ"), List(TriplePattern(Value("subject"), PositionalField(1), Value
          ("object"))))),
      (pattern,
        "object",
        BGPFilter(Pipe("pipeEgkYzrkOZO"), Pipe("a"), List(TriplePattern(PositionalField(0), PositionalField(1), Value
          ("object")))),
        BGPFilter(Pipe("b"), Pipe("pipeEgkYzrkOZO"), List(TriplePattern(Value("subject"), Value("predicate"),
          PositionalField(2))))))

    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only f1
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only f2
    }

    // Don't apply F8 to non-grouped data
    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }

    // Don't apply F8 if one of the variables in the pattern is unbound
    val patternModifier = Table(
      "Modifier",
      (p: TriplePattern) => TriplePattern(PositionalField(0), p.pred, p.obj),
      (p: TriplePattern) => TriplePattern(p.subj, PositionalField(1), p.obj),
      (p: TriplePattern) => TriplePattern(p.subj, p.pred, PositionalField(2))
    )

    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      forAll(patternModifier) { (f: TriplePattern => TriplePattern) =>
        val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
        val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(f(p)))
        val op3 = Dump(Pipe("b"))
        val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
        plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
        plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
      }
    }

    // Don't apply F8 if there's more than one pattern
    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p, p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }

    // Don't apply F8 if there are no patterns
    forAll(patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List.empty)
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }
  }

  it should "apply rewriting rule J1" in {
    Rewriter unless plainSchemaJoinEarlyAbort applyRule J1
    val patterns = Table(
      ("patterns", "filters", "join", "foreach", "expected schema"),
      (List(
        TriplePattern(NamedField("s"), PositionalField(1), Value("obj1")),
        TriplePattern(NamedField("s"), PositionalField(1), Value("obj2"))),
        List(
          BGPFilter(Pipe("pipePClecYbNXF"), Pipe("a"), List(TriplePattern(NamedField("s"), PositionalField(1),
            Value("obj1")))),
          BGPFilter(Pipe("pipevHyYGvOfsZ"), Pipe("a"), List(TriplePattern(NamedField("s"), PositionalField(1),
            Value("obj2"))))),
        Join(Pipe("pipeEgkYzrkOZO"), List(Pipe("pipePClecYbNXF"), Pipe("pipevHyYGvOfsZ")),
          List(List(NamedField("subject")), List(NamedField("subject")))),
        Foreach(Pipe("b"), Pipe("pipeEgkYzrkOZO"), GeneratorList(List(
          GeneratorExpr(
            RefExpr(
              NamedField("subject", List("pipePClecYbNXF"))
            ),
            Some(Field("s", Types.CharArrayType))
          )
        ))),
        Some(Schema(BagType(TupleType(Array(Field("s", Types.CharArrayType))), "b")))
        ),
      (List(
        TriplePattern(PositionalField(0), NamedField("p"), Value("obj1")),
        TriplePattern(PositionalField(0), NamedField("p"), Value("obj2"))),
        List(
          BGPFilter(Pipe("pipePClecYbNXF"), Pipe("a"), List(TriplePattern(PositionalField(0), NamedField("p"),
            Value("obj1")))),
          BGPFilter(Pipe("pipevHyYGvOfsZ"), Pipe("a"), List(TriplePattern(PositionalField(0), NamedField("p"),
            Value("obj2"))))),
        Join(Pipe("pipeEgkYzrkOZO"), List(Pipe("pipePClecYbNXF"), Pipe("pipevHyYGvOfsZ")),
          List(List(NamedField("predicate")), List (NamedField("predicate")))),
        Foreach(Pipe("b"), Pipe("pipeEgkYzrkOZO"), GeneratorList(List(
          GeneratorExpr(
            RefExpr(
              NamedField("predicate", List("pipePClecYbNXF"))
            ),
            Some(Field("p", Types.CharArrayType))
          )
        ))),
        Some(Schema(BagType(TupleType(Array(Field("p", Types.CharArrayType))), "b")))
        ),
      (List(
        TriplePattern(PositionalField(0), Value("pred1"), NamedField("o")),
        TriplePattern(PositionalField(0), Value("pred2"), NamedField("o"))),
        List(
          BGPFilter(Pipe("pipePClecYbNXF"), Pipe("a"), List(TriplePattern(PositionalField(0), Value("pred1"), NamedField
            ("o")))),
          BGPFilter(Pipe("pipevHyYGvOfsZ"), Pipe("a"), List(TriplePattern(PositionalField(0), Value("pred2"), NamedField
            ("o"))))),
        Join(Pipe("pipeEgkYzrkOZO"), List(Pipe("pipePClecYbNXF"), Pipe("pipevHyYGvOfsZ")),
          List(List(NamedField("object")), List (NamedField("object")))),
        Foreach(Pipe("b"), Pipe("pipeEgkYzrkOZO"), GeneratorList(List(
          GeneratorExpr(
            RefExpr(
              NamedField("object", List("pipePClecYbNXF"))
            ),
            Some(Field("o", Types.CharArrayType))
          )
        ))),
        Some(Schema(BagType(TupleType(Array(Field("o", Types.CharArrayType))), "b")))
        )
    )

    forAll(patterns) { (p: List[TriplePattern], fs: List[BGPFilter], j: Join, fo: Foreach, sc: Option[Schema]) =>
      Random.setSeed(123456789)
      PipeNameGenerator.clearGenerated

      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), p)
      val op3 = Dump(Pipe("b"))
      val plan = new DataflowPlan(List(op1, op2, op3))
      val rewrittenPlan = processPlan(plan)
      rewrittenPlan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain theSameElementsAs fs
      rewrittenPlan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only fo
      rewrittenPlan.findOperatorForAlias("b").headOption.value.inputs.map(_.producer) should contain only j
      sc shouldBe rewrittenPlan.findOperatorForAlias("b")
        .headOption.value.schema
    }

    // Don't apply J1 if there's only one pattern
    forAll(patterns) { (p: List[TriplePattern], fs: List[BGPFilter], j: Join, fo: Foreach, sc: Option[Schema]) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p.head))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }

    // Don't apply J1 if there's no pattern
    forAll(patterns) { (p: List[TriplePattern], fs: List[BGPFilter], j: Join, fo: Foreach, sc: Option[Schema]) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List.empty)
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }
  }

  it should "apply rewriting rule J2" in {
    Rewriter unless groupedSchemaJoinEarlyAbort applyRule J2
    val patterns = Table(
      ("Patterns", "ForEach", "Filter"),
      (List(
        TriplePattern(NamedField("s"), PositionalField(1), Value("obj1")),
        TriplePattern(NamedField("s"), PositionalField(1), Value("obj2"))),
        Foreach(Pipe("pipePClecYbNXF"), Pipe("a"), GeneratorPlan(List(
          Filter(Pipe("pipevHyYGvOfsZ"), Pipe("stmts_1"),
            Eq(RefExpr(NamedField("object")), RefExpr(Value("obj1")))),
          Filter(Pipe("pipeEgkYzrkOZO"), Pipe("stmts_2"),
            Eq(RefExpr(NamedField("object")), RefExpr(Value("obj2")))),
          Generate(
            List(
              GeneratorExpr(RefExpr(NamedField("*"))),
              GeneratorExpr(Func("COUNT",
                List(RefExpr(NamedField("pipevHyYGvOfsZ")))),
                Some(Field("cnt0", Types.ByteArrayType))),
              GeneratorExpr(Func("COUNT",
                List(RefExpr(NamedField("pipeEgkYzrkOZO")))),
                Some(Field("cnt1", Types.ByteArrayType)))))))),
        Filter(Pipe("b"), Pipe("pipePClecYbNXF"),
          And(
            Gt(RefExpr(NamedField("cnt0")), RefExpr(Value(0))),
            Gt(RefExpr(NamedField("cnt1")), RefExpr(Value(0))))
        )),

      (List(
          TriplePattern(Value("subj1"), NamedField("p"), PositionalField(3)),
          TriplePattern(Value("subj2"), NamedField("p"), PositionalField(3))),
        Foreach(Pipe("pipePClecYbNXF"), Pipe("a"), GeneratorPlan(List(
          Filter(Pipe("pipevHyYGvOfsZ"), Pipe("stmts_1"),
            Eq(RefExpr(NamedField("subject")), RefExpr(Value("subj1")))),
          Filter(Pipe("pipeEgkYzrkOZO"), Pipe("stmts_2"),
            Eq(RefExpr(NamedField("subject")), RefExpr(Value("subj2")))),
          Generate(
            List(
              GeneratorExpr(RefExpr(NamedField("*"))),
              GeneratorExpr(Func("COUNT",
                List(RefExpr(NamedField("pipevHyYGvOfsZ")))),
                Some(Field("cnt0", Types.ByteArrayType))),
              GeneratorExpr(Func("COUNT",
                List(RefExpr(NamedField("pipeEgkYzrkOZO")))),
                Some(Field("cnt1", Types.ByteArrayType)))))))),
        Filter(Pipe("b"), Pipe("pipePClecYbNXF"),
          And(
            Gt(RefExpr(NamedField("cnt0")), RefExpr(Value(0))),
            Gt(RefExpr(NamedField("cnt1")), RefExpr(Value(0))))
        )),

      (List(
        TriplePattern(PositionalField(0), Value("pred1"), NamedField("o")),
        TriplePattern(PositionalField(0), Value("pred2"), NamedField("o"))),
        Foreach(Pipe("pipePClecYbNXF"), Pipe("a"), GeneratorPlan(List(
          Filter(Pipe("pipevHyYGvOfsZ"), Pipe("stmts_1"),
            Eq(RefExpr(NamedField("predicate")), RefExpr(Value("pred1")))),
          Filter(Pipe("pipeEgkYzrkOZO"), Pipe("stmts_2"),
            Eq(RefExpr(NamedField("predicate")), RefExpr(Value("pred2")))),
          Generate(
            List(
              GeneratorExpr(RefExpr(NamedField("*"))),
              GeneratorExpr(Func("COUNT",
                List(RefExpr(NamedField("pipevHyYGvOfsZ")))),
                Some(Field("cnt0", Types.ByteArrayType))),
              GeneratorExpr(Func("COUNT",
                List(RefExpr(NamedField("pipeEgkYzrkOZO")))),
                Some(Field("cnt1", Types.ByteArrayType)))))))),
        Filter(Pipe("b"), Pipe("pipePClecYbNXF"),
          And(
            Gt(RefExpr(NamedField("cnt0")), RefExpr(Value(0))),
            Gt(RefExpr(NamedField("cnt1")), RefExpr(Value(0))))
        )))

    val possibleGroupers = Table(("grouping column"), ("subject"), ("predicate"), ("object"))

    val wrapped = buildTypedCaseWrapper(J2 _)

    forAll (possibleGroupers) { (g: String) =>
      forAll(patterns) { (p: List[TriplePattern], fo: Foreach, fi: Filter) =>
        // We generate multiple pipe names for each `p`, therefore we need to reset the random generators state not
        // at the beginning of this function, but before processing each combination of pattern and grouping column.
        Random.setSeed(123456789)
        PipeNameGenerator.clearGenerated
        val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
        val op2 = BGPFilter(Pipe("b"), Pipe("a"), p)
        val op3 = Dump(Pipe("b"))
        val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))

        plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only fo
        plan.findOperatorForAlias("b").value.outputs.flatMap(_.consumer) should contain only op3
        plan.findOperatorForAlias("b").value shouldBe fi
        plan.operators should contain only(op1, fo, fi, op3)
      }
    }
  }

  it should "apply rewriting rule J3" in {
    Rewriter unless plainSchemaJoinEarlyAbort applyRule J3
    val patterns = Table(
      ("patterns", "filters", "join", "foreach", "expected schema"),
      (List(
        TriplePattern(NamedField("s"), PositionalField(1), Value("obj1")),
        TriplePattern(PositionalField(0), NamedField("s"), Value("obj2"))),
        List(
          BGPFilter(Pipe("pipePClecYbNXF"), Pipe("a"), List(TriplePattern(NamedField("s"), PositionalField(1),
            Value("obj1")))),
          BGPFilter(Pipe("pipevHyYGvOfsZ"), Pipe("a"), List(TriplePattern(PositionalField(0), NamedField("s"),
            Value("obj2"))))),
        Join(Pipe("pipeEgkYzrkOZO"), List(Pipe("pipePClecYbNXF"), Pipe("pipevHyYGvOfsZ")),
          List(List(NamedField("subject")), List(NamedField("predicate")))),
        Foreach(Pipe("b"), Pipe("pipeEgkYzrkOZO"), GeneratorList(List(
          GeneratorExpr(
            RefExpr(
              NamedField("subject", List("pipePClecYbNXF"))
            ),
            Some(Field("s", Types.CharArrayType))
          )
        ))),
        Some(Schema(BagType(TupleType(Array(Field("s", Types.CharArrayType))), "b")))
        ),
      (List(
        TriplePattern(PositionalField(0), NamedField("p"), Value("obj1")),
        TriplePattern(PositionalField(0), PositionalField(1), NamedField("p"))),
        List(
          BGPFilter(Pipe("pipePClecYbNXF"), Pipe("a"), List(TriplePattern(PositionalField(0), NamedField("p"),
            Value("obj1")))),
          BGPFilter(Pipe("pipevHyYGvOfsZ"), Pipe("a"), List(TriplePattern(PositionalField(0), PositionalField(1),
            NamedField("p"))))),
        Join(Pipe("pipeEgkYzrkOZO"), List(Pipe("pipePClecYbNXF"), Pipe("pipevHyYGvOfsZ")),
          List(List(NamedField("predicate")), List (NamedField("object")))),
        Foreach(Pipe("b"), Pipe("pipeEgkYzrkOZO"), GeneratorList(List(
          GeneratorExpr(
            RefExpr(
              NamedField("predicate", List("pipePClecYbNXF"))
            ),
            Some(Field("p", Types.CharArrayType))
          )
        ))),
        Some(Schema(BagType(TupleType(Array(Field("p", Types.CharArrayType))), "b")))
        ),
      (List(
        TriplePattern(NamedField("o"), Value("pred1"), PositionalField(2)),
        TriplePattern(PositionalField(0), Value("pred2"), NamedField("o"))),
        List(
          BGPFilter(Pipe("pipePClecYbNXF"), Pipe("a"), List(TriplePattern(NamedField("o"), Value("pred1"),
            PositionalField(2)))),
          BGPFilter(Pipe("pipevHyYGvOfsZ"), Pipe("a"), List(TriplePattern(PositionalField(0), Value("pred2"), NamedField
            ("o"))))),
        Join(Pipe("pipeEgkYzrkOZO"), List(Pipe("pipePClecYbNXF"), Pipe("pipevHyYGvOfsZ")),
          List(List(NamedField("subject")), List(NamedField("object")))),
        Foreach(Pipe("b"), Pipe("pipeEgkYzrkOZO"), GeneratorList(List(
          GeneratorExpr(
            RefExpr(
              NamedField("subject", List("pipePClecYbNXF"))
            ),
            Some(Field("o", Types.CharArrayType))
          )
        ))),
        Some(Schema(BagType(TupleType(Array(Field("o", Types.CharArrayType))), "b")))
        )
    )

    forAll(patterns) { (p: List[TriplePattern], fs: List[BGPFilter], j: Join, fo: Foreach, sc: Option[Schema]) =>
      Random.setSeed(123456789)
      PipeNameGenerator.clearGenerated

      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), p)
      val op3 = Dump(Pipe("b"))
      val plan = new DataflowPlan(List(op1, op2, op3))
      val rewrittenPlan = processPlan(plan)
      rewrittenPlan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain theSameElementsAs fs
      rewrittenPlan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only fo
      rewrittenPlan.findOperatorForAlias("b").headOption.value.inputs.map(_.producer) should contain only j
      sc shouldBe rewrittenPlan.findOperatorForAlias("b")
        .headOption.value.schema
    }


    // Don't apply J3 if there's only one pattern
    forAll(patterns) { (p: List[TriplePattern], fs: List[BGPFilter], j: Join, fo: Foreach, sc: Option[Schema]) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p.head))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }

    // Don't apply J3 if there's no pattern
    forAll(patterns) { (p: List[TriplePattern], fs: List[BGPFilter], j: Join, fo: Foreach, sc: Option[Schema]) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List.empty)
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }
  }

  it should "apply rewriting rule J4" in {
    Rewriter unless groupedSchemaJoinEarlyAbort applyRule J4
    val patterns = Table(
      ("patterns", "filters", "flattenning foreachs", "join", "foreach", "expected schema"),
      (List(
        TriplePattern(NamedField("s"), PositionalField(1), Value("obj1")),
        TriplePattern(PositionalField(0), NamedField("s"), Value("obj2"))),
        List(
          BGPFilter(Pipe("pipePClecYbNXF"), Pipe("a"), List(TriplePattern(NamedField("s"), PositionalField(1),
            Value("obj1")))),
          BGPFilter(Pipe("pipevHyYGvOfsZ"), Pipe("a"), List(TriplePattern(PositionalField(0), NamedField("s"),
            Value("obj2"))))),
        List(
          Foreach(Pipe("pipeEgkYzrkOZO"), Pipe("pipePClecYbNXF"),
            GeneratorList(
              List(
                GeneratorExpr(
                  RefExpr(
                    NamedField("subject"))),
                GeneratorExpr(
                  FlattenExpr(
                    RefExpr(
                      NamedField("stmts"))))))),
          Foreach(Pipe("pipejvdcHuREqz"), Pipe("pipevHyYGvOfsZ"),
            GeneratorList(
              List(
                GeneratorExpr(
                  RefExpr(
                    NamedField("subject"))),
                GeneratorExpr(
                  FlattenExpr(
                    RefExpr(
                      NamedField("stmts")))))))
        ),
        Join(Pipe("pipeUxwEkfQHGx"), List(Pipe("pipeEgkYzrkOZO"), Pipe("pipejvdcHuREqz")),
          List(List(NamedField("subject")), List(NamedField("predicate")))),
        Foreach(Pipe("b"), Pipe("pipeUxwEkfQHGx"), GeneratorList(List(
          GeneratorExpr(
            RefExpr(
              NamedField("subject", List("pipeEgkYzrkOZO"))
            ),
            Some(Field("s", Types.CharArrayType))
          )
        ))),
        Some(Schema(BagType(TupleType(Array(Field("s", Types.CharArrayType))), "b")))
        ),
      (List(
        TriplePattern(PositionalField(0), NamedField("p"), Value("obj1")),
        TriplePattern(PositionalField(0), PositionalField(1), NamedField("p"))),
        List(
          BGPFilter(Pipe("pipePClecYbNXF"), Pipe("a"), List(TriplePattern(PositionalField(0), NamedField("p"),
            Value("obj1")))),
          BGPFilter(Pipe("pipevHyYGvOfsZ"), Pipe("a"), List(TriplePattern(PositionalField(0), PositionalField(1),
            NamedField("p"))))),
        List(
          Foreach(Pipe("pipeEgkYzrkOZO"), Pipe("pipePClecYbNXF"),
            GeneratorList(
              List(
                GeneratorExpr(
                  RefExpr(
                    NamedField("subject"))),
                GeneratorExpr(
                  FlattenExpr(
                    RefExpr(
                      NamedField("stmts"))))))),
          Foreach(Pipe("pipejvdcHuREqz"), Pipe("pipevHyYGvOfsZ"),
            GeneratorList(
              List(
                GeneratorExpr(
                  RefExpr(
                    NamedField("subject"))),
                GeneratorExpr(
                  FlattenExpr(
                    RefExpr(
                      NamedField("stmts")))))))
        ),
        Join(Pipe("pipeUxwEkfQHGx"), List(Pipe("pipeEgkYzrkOZO"), Pipe("pipejvdcHuREqz")),
          List(List(NamedField("predicate")), List (NamedField("object")))),
        Foreach(Pipe("b"), Pipe("pipeUxwEkfQHGx"), GeneratorList(List(
          GeneratorExpr(
            RefExpr(
              NamedField("predicate", List("pipeEgkYzrkOZO"))
            ),
            Some(Field("p", Types.CharArrayType))
          )
        ))),
        Some(Schema(BagType(TupleType(Array(Field("p", Types.CharArrayType))), "b")))
        ),
      (List(
        TriplePattern(NamedField("o"), Value("pred1"), PositionalField(2)),
        TriplePattern(PositionalField(0), Value("pred2"), NamedField("o"))),
        List(
          BGPFilter(Pipe("pipePClecYbNXF"), Pipe("a"), List(TriplePattern(NamedField("o"), Value("pred1"),
            PositionalField(2)))),
          BGPFilter(Pipe("pipevHyYGvOfsZ"), Pipe("a"), List(TriplePattern(PositionalField(0), Value("pred2"), NamedField
            ("o"))))),
        List(
          Foreach(Pipe("pipeEgkYzrkOZO"), Pipe("pipePClecYbNXF"),
            GeneratorList(
              List(
                GeneratorExpr(
                  RefExpr(
                    NamedField("subject"))),
                GeneratorExpr(
                  FlattenExpr(
                    RefExpr(
                      NamedField("stmts"))))))),
          Foreach(Pipe("pipejvdcHuREqz"), Pipe("pipevHyYGvOfsZ"),
            GeneratorList(
              List(
                GeneratorExpr(
                  RefExpr(
                    NamedField("subject"))),
                GeneratorExpr(
                  FlattenExpr(
                    RefExpr(
                      NamedField("stmts")))))))
        ),
        Join(Pipe("pipeUxwEkfQHGx"), List(Pipe("pipeEgkYzrkOZO"), Pipe("pipejvdcHuREqz")),
          List(List(NamedField("subject")), List(NamedField("object")))),
        Foreach(Pipe("b"), Pipe("pipeUxwEkfQHGx"), GeneratorList(List(
          GeneratorExpr(
            RefExpr(
              NamedField("subject", List("pipeEgkYzrkOZO"))
            ),
            Some(Field("o", Types.CharArrayType))
          )
        ))),
        Some(Schema(BagType(TupleType(Array(Field("o", Types.CharArrayType))), "b")))
        )
    )

    forAll(patterns) { (p: List[TriplePattern], fs: List[BGPFilter], f_fo: List[Foreach], j: Join, fo: Foreach, sc:
    Option[Schema]) =>
      Random.setSeed(123456789)
      PipeNameGenerator.clearGenerated

      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some("subject")) // TODO group by other columns
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), p)
      val op3 = Dump(Pipe("b"))
      val plan = new DataflowPlan(List(op1, op2, op3))
      val rewrittenPlan = processPlan(plan)
      rewrittenPlan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain theSameElementsAs fs
      rewrittenPlan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only fo
      rewrittenPlan.findOperatorForAlias("b").headOption.value.inputs.map(_.producer) should contain only j
      rewrittenPlan.findOperatorForAlias("pipeUxwEkfQHGx").headOption.value.inputs.map(_.producer) should contain theSameElementsAs f_fo
      sc shouldBe rewrittenPlan.findOperatorForAlias("b")
        .headOption.value.schema
    }


    // Don't apply J4 if there's only one pattern
    forAll(patterns) { (p: List[TriplePattern], fs: List[BGPFilter], f_fo: List[Foreach], j: Join, fo: Foreach, sc:
      Option[Schema]) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some("subject"))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p.head))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }

    // Don't apply J4 if there's no pattern
    forAll(patterns) { (p: List[TriplePattern], fs: List[BGPFilter], f_fo: List[Foreach], j: Join, fo: Foreach, sc:
    Option[Schema]) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some("subject"))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List.empty)
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }
  }

  it should "replace GENERATE * by a list of fields" in {
    addOperatorReplacementStrategy(foreachGenerateWithAsterisk)
    val plan = new DataflowPlan(parseScript(
      s"""A = LOAD 'file' AS (x, y, z);
         |B = FOREACH A GENERATE *;
         |DUMP B;
       """.stripMargin))
    val rewrittenPlan = processPlan(plan)
    val op = rewrittenPlan.findOperatorForAlias("B")
    op should be (Some(Foreach(Pipe("B"),Pipe("A"),
      GeneratorList(List(GeneratorExpr(RefExpr(NamedField("x"))),
        GeneratorExpr(RefExpr(NamedField("y"))), GeneratorExpr(RefExpr(NamedField("z"))))))))
  }

  it should "replace GENERATE *, fields" in {
    addOperatorReplacementStrategy(foreachGenerateWithAsterisk)
    val plan = new DataflowPlan(parseScript(
      "A = LOAD 'file' AS (x, y, z);\nB = FOREACH A GENERATE *, $0, $2;\nDUMP B;"))
    val rewrittenPlan = processPlan(plan)
    val op = rewrittenPlan.findOperatorForAlias("B")
    op should be (Some(Foreach(Pipe("B"),Pipe("A"),
      GeneratorList(List(GeneratorExpr(RefExpr(NamedField("x"))),
        GeneratorExpr(RefExpr(NamedField("y"))),
        GeneratorExpr(RefExpr(NamedField("z"))),
        GeneratorExpr(RefExpr(PositionalField(0))),
        GeneratorExpr(RefExpr(PositionalField(2)))
      )))))
  }

  it should "replace GENERATE * in a nested FOREACH" in {
    addOperatorReplacementStrategy(foreachGenerateWithAsterisk)
    addStrategy(removeNonStorageSinks _)
    val plan = new DataflowPlan(parseScript(
      """triples = LOAD 'file' AS (sub, pred, obj);
         |stmts = GROUP triples BY sub;
         |tmp = FOREACH stmts {
         |r1 = FILTER triples BY (pred == 'aPred1');
         |r2 = FILTER triples BY (pred == 'aPred2');
         |GENERATE *, COUNT(r1) AS cnt1, COUNT(r2) AS cnt2;
         |};""".stripMargin))
    val rewrittenPlan = processPlan(plan)
  }

  "pullOpAcrossMultipleInputOp" should "throw an exception if toBePulled is not a consumer of multipleInputOp" in {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val predicate1 = Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))
    val op2 = OrderBy(Pipe("b"), Pipe("a"), List())
    val op3 = Filter(Pipe("c"), Pipe("b"), predicate1)
    val op4 = Dump(Pipe("c"))

    // This sets up pipes etc.
    val plan = new DataflowPlan(List(op1, op2, op3, op4))
    intercept[IllegalArgumentException] {
      pullOpAcrossMultipleInputOp(op4, op2, op1)
    }
  }

  it should "pull up toBePulled if it's a consumer of multipleInputOps output pipes" in {
    val plan = new DataflowPlan(
      parseScript("""A = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (a1:int,a2:int, a3:int);
                  | B = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (b1:int,b2:int,b3:int);
                  | X = JOIN A BY a1, B BY b1;
                  | C = FILTER X by a1 > 2;
                  | DUMP C;""".stripMargin))

    val op1 = plan.findOperatorForAlias("A").value
    val op1_2 = plan.findOperatorForAlias("B").value
    val op2 = plan.findOperatorForAlias("X").value
    val op3 = plan.findOperatorForAlias("C").value
    val op4 = plan.sinkNodes.head
    val indexOfPipeFromLoadToJoin = op2.inputs.indexWhere(_.producer == op1)

    pullOpAcrossMultipleInputOp(op3, op2, op1)
    val indexOfPipeFromLoadToFilter = op2.inputs.indexWhere(_.producer == op3)

    // A -> C, from both sides
    op1.outputs.flatMap(_.consumer) should contain only op3
    op3.inputs.map(_.producer) should contain only op1
    // C -> X, from both sides ...
    op3.outputs.flatMap(_.consumer) should contain only op2
    // ... and B -> X
    op2.inputs.map(_.producer) should contain only (op3, op1_2)
    // X -> DUMP, from both sides
    op2.outputs.flatMap(_.consumer) should contain only op4
    op4.inputs.map(_.producer) should contain only op2

    // The pipe from the (now pulled up) filter operation should be at the same position as the one from op1 was
    indexOfPipeFromLoadToFilter shouldBe indexOfPipeFromLoadToJoin
    op3.schema shouldBe op1.schema
  }

  "ForEachCallingFunctionE" should "extract the function name of a function called in the only GeneratorExpr of a" +
    " GeneratorList in a ForEach statement" in {
    val p = new PigParser()
    val op = p.parseScript("B = FOREACH A GENERATE myFunc(f1, f2);").head
    op should matchPattern {
      case ForEachCallingFunctionE(_, "myFunc") =>
    }

    val op2 = p.parseScript("B = FOREACH A GENERATE notMyFunc(f1, f2);").head
    op2 should not matchPattern {
      case ForEachCallingFunctionE(_, "myFunc") =>
    }
  }

  "SuccE" should "extract the single successor of a PigOperator" in {
    val p = new PigParser()
    val ops = p.parseScript(
      """
        | a = load 'foo' using PigStorage(':');
        | dump a;
      """.stripMargin)
    val load = ops.headOption.value
    val dump = ops.last

    new DataflowPlan(ops)

    load should matchPattern {
      case SuccE(load, dump) =>
    }

    dump should not matchPattern {
      case SuccE(_) =>
    }
  }

  "AllSuccE" should "extract all successors of a PigOperator" in {
    val p = new PigParser()
    val ops = p.parseScript(
      """
        | a = load 'foo' using PigStorage(':');
        | b = filter a by $0 == 'hallo';
        | dump b;
        | dump a;
      """.stripMargin)
    val load = ops.headOption.value
    val b = ops(1)
    val dump = ops(3)

    new DataflowPlan(ops)

    load should matchPattern {
      case AllSuccE(load, b :: dump) =>
    }

    dump should matchPattern {
      case AllSuccE(dump, Nil) =>
    }
  }
  "The PipeNameGenerator" should "not generate duplicate pipe names" in {
    val seed = 1234567890
    Random.setSeed(seed)
    val gen1 = PipeNameGenerator.generate()

    // Reset the seed, so that the generator generates the same name again
    Random.setSeed(seed)
    val gen2 = PipeNameGenerator.generate()

    gen1 should not equal gen2
  }

  "The SplitIntoToFilters rule" should "rewrite SplitInto operators into multiple Filter ones" in {
    addStrategy(strategyf(t => splitIntoToFilters(t)))
    val plan = new DataflowPlan(parseScript( s"""
                                                |a = LOAD 'file' AS (x, y);
                                                |SPLIT a INTO b IF x < 100, c IF x >= 100;
                                                |STORE b INTO 'res1.data';
                                                |STORE c INTO 'res2.data';""".stripMargin))

    val filter1 = parseScript("b = filter a by x < 100;").head
    val filter2 = parseScript("c = filter a by x >= 100;").head
    val newPlan = processPlan(plan)

    newPlan.sourceNodes.headOption.value.outputs.head.consumer should have length 2
    newPlan.sourceNodes.headOption.value.outputs.head.consumer should contain allOf(filter1, filter2)

    newPlan.sinkNodes should have size 2
    newPlan.sinkNodes.foreach(node => {
      node.inputs should have length 1
    })

    val sink1 = newPlan.sinkNodes.head
    val sink2 = newPlan.sinkNodes.last

    sink1.inputs.head.producer should be(if (sink1.inputs.head.name == "b") filter1 else filter2)
    sink2.inputs.head.producer should be(if (sink2.inputs.head.name == "c") filter2 else filter1)
  }

  it should "not behave unreasonably if the next operator is a join" in {
    addStrategy(strategyf(t => splitIntoToFilters(t)))
    val plan = new DataflowPlan(parseScript( s"""
                                                |a = LOAD 'file' AS (x, y);
                                                |SPLIT a INTO b IF x < 100, c IF x >= 100;
                                                |d = JOIN b by x, c by x;
                                                |store d into 'res.data';""".stripMargin))

    var dOp = plan.findOperatorForAlias("d").value
    dOp.inputs should have length 2
    dOp.inputs.map(_.producer) should have length 2

    val newPlan = processPlan(plan)

    newPlan.sourceNodes.headOption.value.outputs.head.consumer should have length 2
    dOp = newPlan.findOperatorForAlias("d").value
    dOp.inputs should have length 2
    dOp.inputs.map(_.producer) should have length 2
  }

  "The Rewriter DSL" should "apply patterns via applyPattern without conditions" in {
    Rewriter applyPattern { case SuccE(o: OrderBy, succ: Filter) => Functions.swap(o, succ) }
    performReorderingTest()
  }

  it should "apply patterns via applyPattern with a condition added by when" in {
    Rewriter when { t: OrderBy => t.outputs.length > 0 } applyPattern {
      case SuccE(o: OrderBy, succ: Filter) => Functions.swap(o, succ)
    }
    performReorderingTest()
  }

  it should "apply patterns via applyPattern with a condition added by unless" in {
    Rewriter unless { t: OrderBy => t.outputs.length == 0 } applyPattern {
      case SuccE(o: OrderBy, succ: Filter) => Functions.swap(o, succ)
    }
    performReorderingTest()
  }

  it should "allow merging operators" in {
    Rewriter toMerge(classOf[Filter], classOf[Filter]) whenMatches {
      case (f1 @ Filter(_, _, pred1, _), f2 @ Filter(_, _, pred2, _)) if pred1 != pred2 => } applyRule {
        tup: (Filter, Filter) => mergeFilters(tup._1, tup ._2)
      }
    performMergeTest()
  }

  it should "allow removing operators" in {
    Rewriter applyPattern { case SuccE(o: OrderBy, f: Filter) => f }
    performRemovalTest()
  }

  "Functions" should "allow merging operators" in {
    Rewriter toMerge(classOf[Filter], classOf[Filter]) applyRule { case (t1: Filter, t2: Filter) =>
      def merger(f1: Filter, f2: Filter) = Filter(f2.outputs.head, f1.inputs.head, And(f1.pred, f2.pred))
      Some(Functions.merge(t1, t2, merger))
    }
    performMergeTest()
  }

  it should "allow swapping operators" in {
    Rewriter unless { t: OrderBy => t.outputs.length == 0 } applyPattern {
      case SuccE(o: OrderBy, succ: Filter) => Functions.swap(o, succ)
    }
    performReorderingTest()
  }

  it should "allow removing operators" in {
    Rewriter applyPattern {case op : OrderBy => Functions.remove(op)}
    performRemovalTest()
  }

  // This is the last test because it takes by far the longest. Please keep it down here to reduce waiting times for
  // other test results :-)
  "Embedsupport" should "apply rules registered by embedded code" in {
    val p = new PigParser()
    val ops = p.parseScript(
      """
        |<! def myFunc(s: String): String = {
        |   s
        | }
        | rules:
        | import dbis.pig.plan.rewriting.Rewriter
        | def rule(op: Any): Option[PigOperator] = {
        | op match {
        |   case ForEachCallingFunctionE(_, "myFunc") =>
        |     val fo = op.asInstanceOf[Foreach]
        |     Some(Distinct(fo.outputs.head, fo.inputs.head))
        |   case _ =>
        |     None
        | }
        | }
        | List(buildOperatorReplacementStrategy(rule))
        |!>
        |a = LOAD 'file.csv';
        |b = FOREACH a GENERATE myFunc($0);
        |dump b;
      """.stripMargin, LanguageFeature.PlainPig)
    val plan = new DataflowPlan(ops)
    plan.extraRuleCode should have length 1
    val newPlan = processPlan(plan)
    newPlan.operators should contain only(plan.sourceNodes.headOption.value,
      plan.sinkNodes.headOption.value,
      Distinct(Pipe("b"), Pipe("a")))
  }

}
