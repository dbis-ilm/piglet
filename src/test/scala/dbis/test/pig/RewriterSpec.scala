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
import dbis.pig.parser.PigParser
import dbis.pig.plan.DataflowPlan
import dbis.pig.plan.rewriting.Extractors.{OnlyFollowedByE, ForEachCallingFunctionE}
import dbis.pig.plan.rewriting.Rewriter._
import dbis.pig.plan.rewriting.internals.PipeNameGenerator
import dbis.pig.plan.rewriting.{Rewriter, Rules}
import dbis.pig.schema.{BagType, Schema, TupleType, _}
import dbis.test.TestTools._
import org.kiama.rewriting.Rewriter.{strategyf}
import org.scalatest.OptionValues._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class RewriterSpec extends FlatSpec with Matchers with TableDrivenPropertyChecks {
  "The rewriter" should "merge two Filter operations" in {
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

  it should "remove Filter operation if it has the same predicate as an earlier one" in {
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

  it should "order Filter operations before Joins if only NamedFields are used" in {
    val op1 = Load(Pipe("a"), "input/file.csv", Some(Schema(BagType(TupleType(Array(Field("a", Types.IntType), Field("aid", Types.IntType)))
    ))))
    val op2 = Load(Pipe("b"), "file2.csv", Some(Schema(BagType(TupleType(Array(Field("b", Types.CharArrayType), Field
      ("bid", Types.IntType)))
    ))))
    val predicate1 = Lt(RefExpr(NamedField("a")), RefExpr(Value("42")))

    // ops before reordering
    val op3 = Join(Pipe("c"), List(Pipe("a"), Pipe("b")),
      List(List(NamedField("aid")),
           List(NamedField("bid"))
      ))
    val op4 = Filter(Pipe("d"), Pipe("c"), predicate1)
    val op5 = Dump(Pipe("d"))

    val plan = processPlan(new DataflowPlan(List(op1, op2, op3, op4, op5)))
    op1.outputs.headOption.value.consumer should contain only op4
    op2.outputs.headOption.value.consumer should contain only op3
    op4.outputs.headOption.value.consumer should contain only(op3, op5)

    op1.outputs should have length 1
    op2.outputs should have length 1
    op3.outputs should have length 1
    op4.outputs should have length 1

    plan.findOperatorForAlias("c").headOption.value.inputs.map(_.producer) should contain only(op4, op2)
  }

  it should "order Filter operations before Cross operators if only NamedFields are used" in {
    val op1 = Load(Pipe("a"), "input/file.csv", Some(Schema(BagType(TupleType(Array(Field("a", Types.IntType), Field("aid", Types.IntType)))
    ))))
    val op2 = Load(Pipe("b"), "file2.csv", Some(Schema(BagType(TupleType(Array(Field("b", Types.CharArrayType), Field
      ("bid", Types.IntType)))
    ))))
    val predicate1 = Lt(RefExpr(NamedField("a")), RefExpr(Value("42")))

    // ops before reordering
    val op3 = Cross(Pipe("c"), List(Pipe("a"), Pipe("b")))
    val op4 = Filter(Pipe("d"), Pipe("c"), predicate1)
    val op5 = Dump(Pipe("d"))

    val plan = processPlan(new DataflowPlan(List(op1, op2, op3, op4, op5)))
    op1.outputs.headOption.value.consumer should contain only op4
    op2.outputs.headOption.value.consumer should contain only op3
    op4.outputs.headOption.value.consumer should contain only(op3, op5)

    op1.outputs should have length 1
    op2.outputs should have length 1
    op3.outputs should have length 1
    op4.outputs should have length 1

    plan.findOperatorForAlias("c").headOption.value.inputs.map(_.producer) should contain only(op4, op2)
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
    val op1 = Load(Pipe("a"), "input/file.csv")
    val plan = new DataflowPlan(List(op1))
    val newPlan = processPlan(plan)

    newPlan.sourceNodes shouldBe empty
  }

  it should "remove sink nodes that don't store a relation and have an empty outputs list" in {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val plan = new DataflowPlan(List(op1))

    op1.outputs = List.empty
    val newPlan = processPlan(plan)

    newPlan.sourceNodes shouldBe empty
  }

  it should "pull up Empty nodes" in {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val op2 = OrderBy(Pipe("b"), Pipe("a"), List())
    val plan = new DataflowPlan(List(op1, op2))
    plan.operators should have length 2

    val newPlan = processPlan(plan)

    newPlan.sourceNodes shouldBe empty
    newPlan.operators shouldBe empty
  }

  it should "apply rewriting rule R1" in {
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

  it should "apply rewriting rule L2" in {
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
    val patterns = Table(
      ("Pattern"),
      (TriplePattern(Value("subject"), PositionalField(1), PositionalField(2)),
        Filter(Pipe("b"), Pipe("a"), Eq(RefExpr(NamedField("subject")), RefExpr(Value("subject"))))),
      (TriplePattern(PositionalField(0), Value("predicate"), PositionalField(2)),
      Filter(Pipe("b"), Pipe("a"), Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicate"))))),
      (TriplePattern(PositionalField(0), PositionalField(1), Value("object")),
      Filter(Pipe("b"), Pipe("a"), Eq(RefExpr(NamedField("object")), RefExpr(Value("object"))))))
    forAll (patterns) { (p: (TriplePattern, Filter)) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p._1))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy
        (buildTypedCaseWrapper(Rules.F2)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only p._2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only p._2
    }

    val possibleGroupers = Table(("grouping column"), ("subject"), ("predicate"), ("object"))

    forAll (possibleGroupers) { (g: String) =>
      forAll(patterns) { (p: (TriplePattern, Filter)) =>
        val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
        val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p._1))
        val op3 = Dump(Pipe("b"))
        val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy
          (buildTypedCaseWrapper(Rules.F2)))

        plan.findOperatorForAlias("b").value.outputs.flatMap(_.consumer) should contain only op3
      }
    }
  }

  it should "apply rewriting rule F3" in {
    val patterns = Table(
      ("Pattern"),
      // s p o bound
      (TriplePattern(Value("subject"), Value("predicate"), Value("object")),
        Filter(Pipe("b"), Pipe("a"), And(
          Eq(RefExpr(NamedField("subject")), RefExpr(Value("subject"))),
          And(
            Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicate"))),
            Eq(RefExpr(NamedField("object")), RefExpr(Value("object"))))))),
      // s p bound
      (TriplePattern(Value("subject"), Value("predicate"), PositionalField(2)),
        Filter(Pipe("b"), Pipe("a"), And(
          Eq(RefExpr(NamedField("subject")), RefExpr(Value("subject"))),
          Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicate")))))),
      // s o bound
      (TriplePattern(Value("subject"), PositionalField(1), Value("object")),
        Filter(Pipe("b"), Pipe("a"), And(
          Eq(RefExpr(NamedField("subject")), RefExpr(Value("subject"))),
          Eq(RefExpr(NamedField("object")), RefExpr(Value("object")))))),
      // p o bound
      (TriplePattern(PositionalField(0), Value("predicate"), Value("object")),
        Filter(Pipe("b"), Pipe("a"), And(
          Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicate"))),
          Eq(RefExpr(NamedField("object")), RefExpr(Value("object")))))))

    forAll (patterns) { (p: (TriplePattern, Filter)) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p._1))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy
        (buildTypedCaseWrapper(Rules.F3)))
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
        val plan = processPlan(new DataflowPlan(List(op1, op2, op3, op4)), buildOperatorReplacementStrategy
          (buildTypedCaseWrapper(Rules.F3)))

        plan.findOperatorForAlias("b").value.outputs.flatMap(_.consumer) should contain only op3
      }
    }
  }

  it should "apply rewriting rule F4" in {
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
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy
        (buildTypedCaseWrapper(Rules.F4)))
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
          val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy
            (buildTypedCaseWrapper(Rules.F4)))

          plan.findOperatorForAlias("b").value.outputs.flatMap(_.consumer) should contain only op3
          plan.operators should contain only(op1, op2, op3)
        }
      }
    }
  }

  it should "apply rewriting rule F5" in {
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
          val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F5))

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
           val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F5))

           plan.findOperatorForAlias("b").value.outputs.flatMap(_.consumer) should contain only op3
           plan.operators should contain only(op1, op2, op3)
         }
      }
    }
  }

  it should "apply rewriting rule F6" in {
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
          val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F6))

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
          val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F5))

          plan.findOperatorForAlias("b").value.outputs.flatMap(_.consumer) should contain only op3
          plan.operators should contain only(op1, op2, op3)
        }
      }
    }
  }

  it should "apply rewriting rule F7" in {
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
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy
        (buildTypedCaseWrapper(Rules.F7)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only f1
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only f2
    }

    // Don't apply F7 to non-grouped data
    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy
        (buildTypedCaseWrapper(Rules.F7)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }

    // Don't apply F7 if there's more than one pattern
    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p, p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy
        (buildTypedCaseWrapper(Rules.F7)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }

    // Don't apply F7 if there are no patterns
    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List.empty)
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy
        (buildTypedCaseWrapper(Rules.F7)))
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
          val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy
            (buildTypedCaseWrapper(Rules.F7)))

          plan.findOperatorForAlias("b").value.outputs.flatMap(_.consumer) should contain only op3
          plan.operators should contain only(op1, op2, op3)
        }
      }
    }
  }

  it should "apply rewriting rule F8" in {
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
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(buildTypedCaseWrapper(Rules.F8)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only f1
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only f2
    }

    // Don't apply F8 to non-grouped data
    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(buildTypedCaseWrapper(Rules.F8)))
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
        val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(buildTypedCaseWrapper(Rules.F8)))
        plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
        plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
      }
    }

    // Don't apply F8 if there's more than one pattern
    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p, p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(buildTypedCaseWrapper(Rules.F8)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }

    // Don't apply F8 if there are no patterns
    forAll(patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List.empty)
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(buildTypedCaseWrapper(Rules.F8)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }
  }

  it should "apply rewriting rule J1" in {
    Random.setSeed(123456789)
    PipeNameGenerator.clearGenerated

    val patterns = Table(
      ("patterns", "filters", "join"),
      (List(
        TriplePattern(NamedField("s"), PositionalField(1), Value("obj1")),
        TriplePattern(NamedField("s"), PositionalField(1), Value("obj2"))),
        List(
          BGPFilter(Pipe("pipePClecYbNXF"), Pipe("a"), List(TriplePattern(NamedField("s"), PositionalField(1),
            Value("obj1")))),
          BGPFilter(Pipe("pipevHyYGvOfsZ"), Pipe("a"), List(TriplePattern(NamedField("s"), PositionalField(1),
            Value("obj2"))))),
        Join(Pipe("b"), List(Pipe("pipePClecYbNXF"), Pipe("pipevHyYGvOfsZ")), List(List(NamedField("s")),
          List(NamedField("s"))))),
      (List(
        TriplePattern(PositionalField(0), NamedField("p"), Value("obj1")),
        TriplePattern(PositionalField(0), NamedField("p"), Value("obj2"))),
        List(
          BGPFilter(Pipe("pipeEgkYzrkOZO"), Pipe("a"), List(TriplePattern(PositionalField(0), NamedField("p"),
            Value("obj1")))),
          BGPFilter(Pipe("pipejvdcHuREqz"), Pipe("a"), List(TriplePattern(PositionalField(0), NamedField("p"),
            Value("obj2"))))),
        Join(Pipe("b"), List(Pipe("pipeEgkYzrkOZO"), Pipe("pipejvdcHuREqz")), List(List(NamedField("p")), List
          (NamedField
          ("p"))))),
      (List(
        TriplePattern(PositionalField(0), Value("pred1"), NamedField("o")),
        TriplePattern(PositionalField(0), Value("pred2"), NamedField("o"))),
        List(
          BGPFilter(Pipe("pipeUxwEkfQHGx"), Pipe("a"), List(TriplePattern(PositionalField(0), Value("pred1"), NamedField
            ("o")))),
          BGPFilter(Pipe("pipeYAXKzBIYXu"), Pipe("a"), List(TriplePattern(PositionalField(0), Value("pred2"), NamedField
            ("o"))))),
        Join(Pipe("b"), List(Pipe("pipeUxwEkfQHGx"), Pipe("pipeYAXKzBIYXu")), List(List(NamedField("o")), List
          (NamedField("o")))))
    )

    forAll(patterns) { (p: List[TriplePattern], fs: List[BGPFilter], j: Join) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), p)
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), strategyf(t => Rules.J1(t)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain theSameElementsAs fs
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only j
    }


    // Don't apply J1 if there's only one pattern
    forAll(patterns) { (p: List[TriplePattern], fs: List[BGPFilter], j: Join) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p.head))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), strategyf(t => Rules.J1(t)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }

    // Don't apply J1 if there's no pattern
    forAll(patterns) { (p: List[TriplePattern], fs: List[BGPFilter], j: Join) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List.empty)
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), strategyf(t => Rules.J1(t)))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }
  }

  it should "replace GENERATE * by a list of fields" in {
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
    val op1 = Load(Pipe("a"), "input/file.csv")
    val op2 = OrderBy(Pipe("b"), Pipe("a"), List())
    val predicate1 = Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))
    val op3 = Filter(Pipe("c"), Pipe("b"), predicate1)
    val op4 = Dump(Pipe("c"))

    // This sets up pipes etc.
    new DataflowPlan(List(op1, op2, op3, op4))

    pullOpAcrossMultipleInputOp(op3, op2, op1)

    op1.outputs.flatMap(_.consumer) should contain only op3
    op3.inputs.map(_.producer) should contain only op1
    op3.outputs.flatMap(_.consumer) should contain only op2
    op2.inputs.map(_.producer) should contain only op3
    op2.outputs.flatMap(_.consumer) should contain only op4
    op4.inputs.map(_.producer) should contain only op2
  }

  "The ForEachCallingFunctionE" should "extract the function name of a function called in the only GeneratorExpr of a" +
    " GeneratorList in a ForEach statement" in {
    val p = new PigParser()
    val op = p.parseScript("B = FOREACH A GENERATE myFunc(f1, f2);").head
    op should matchPattern {
      case ForEachCallingFunctionE("myFunc") =>
    }

    val op2 = p.parseScript("B = FOREACH A GENERATE notMyFunc(f1, f2);").head
    op2 should not matchPattern {
      case ForEachCallingFunctionE("myFunc") =>
    }
  }

  "The OnlyFollowedByE" should "extract the single successor of a PigOperator" in {
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
      case OnlyFollowedByE( dump) =>
    }

    dump should not matchPattern {
      case OnlyFollowedByE(_) =>
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
        |   case ForEachCallingFunctionE("myFunc") =>
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
      """.stripMargin)
    val plan = new DataflowPlan(ops)
    plan.extraRuleCode should have length 1
    val newPlan = processPlan(plan)
    newPlan.operators should contain only(plan.sourceNodes.headOption.value,
      plan.sinkNodes.headOption.value,
      Distinct(Pipe("b"), Pipe("a")))
  }

}
