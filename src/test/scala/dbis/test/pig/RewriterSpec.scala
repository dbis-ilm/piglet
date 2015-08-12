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
import dbis.pig.plan.DataflowPlan
import dbis.pig.plan.rewriting.Rewriter._
import dbis.pig.plan.rewriting.Rules
import dbis.pig.schema.{BagType, Schema, TupleType, _}
import dbis.test.TestTools._
import org.scalatest.OptionValues._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class RewriterSpec extends FlatSpec with Matchers with TableDrivenPropertyChecks {
  "The rewriter" should "merge two Filter operations" in {
    val op1 = Load(Pipe("a"), "input/file.csv")
    val predicate1 = Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))
    val predicate2 = Lt(RefExpr(PositionalField(1)), RefExpr(Value("42")))
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

  it should "rewrite SplitInto operators into multiple Filter ones" in {
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
      source shouldBe Load(Pipe("a"), url, op1.schema, "pig.SPARQLLoader",
        List("SELECT * WHERE { ?s ?p ?o }"))
    }
  }

  it should "apply rewriting rule R2" in {
    val op1 = RDFLoad(Pipe("a"), new URI("http://example.com"), None)
    val op2 = BGPFilter(Pipe("b"), Pipe("a"),
      List(
        TriplePattern(
          PositionalField(0),
          Value(""""firstName""""),
          Value(""""Stefan""""))))
    val op3 = Dump(Pipe("b"))
    val plan = processPlan(new DataflowPlan(List(op1, op2, op3)))
    val source = plan.sourceNodes.headOption.value
    assume(false, "The conversion of TriplePatterns to Strings is not yet implemented")
    source shouldBe Load(Pipe("b"), "http://example.com", op1.schema, "pig.SPARQLLoader",
      List("""SELECT * WHERE { $0 "firstName" "Stefan" }"""))
  }

  it should "apply rewriting rule L2" in {
    val possibleGroupers = Table(("grouping column"), ("subject"), ("predicate"), ("object"))
    forAll (possibleGroupers) { (g: String) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = Dump(Pipe("a"))
      val plan = processPlan(new DataflowPlan(List(op1, op2)))
      val source = plan.sourceNodes.headOption.value
      source shouldBe Load(Pipe("a"), "hdfs://somewhere", op1.schema, "BinStorage")
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
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F2))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only p._2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only p._2
    }

    val possibleGroupers = Table(("grouping column"), ("subject"), ("predicate"), ("object"))

    forAll (possibleGroupers) { (g: String) =>
      forAll(patterns) { (p: (TriplePattern, Filter)) =>
        val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
        val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p._1))
        val op3 = Dump(Pipe("b"))
        val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F2))

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
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F3))
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
        val plan = processPlan(new DataflowPlan(List(op1, op2, op3, op4)), buildOperatorReplacementStrategy(Rules.F3))

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
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F4))
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
          val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F4))

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
    val pattern = TriplePattern(Value("subject"),Value("predicate"), Value("object"))
    val patterns = Table(
      ("Pattern", "grouping column", "Grouping column Filter", "Other Filter"),
      // subject & predicate bound, grouped by subject
      (TriplePattern(Value("subject"), Value("predicate"), PositionalField(2)),
        "subject",
        BGPFilter(Pipe("–깍窶䠃ऑ졮硔镔鰛끺"), Pipe("a"), List(TriplePattern(Value("subject"), PositionalField(1),
          PositionalField(2)))),
        BGPFilter(Pipe("b"), Pipe("–깍窶䠃ऑ졮硔镔鰛끺"),
          List(TriplePattern(PositionalField(0), Value("predicate"), PositionalField(2))))),
      // subject & predicate bound, grouped by predicate
      (TriplePattern(Value("subject"), Value("predicate"), PositionalField(2)),
        "predicate",
        BGPFilter(Pipe("⛛⠥톦럫㓚喬詧ǌꏚ蟃"), Pipe("a"), List(TriplePattern(PositionalField(0), Value("predicate"),
          PositionalField(2)))),
        BGPFilter(Pipe("b"), Pipe("⛛⠥톦럫㓚喬詧ǌꏚ蟃"),
          List(TriplePattern(Value("subject"), PositionalField(1), PositionalField(2))))),
      // subject & object bound, grouped by subject
      (TriplePattern(Value("subject"), PositionalField(1), Value("object")),
        "subject",
        BGPFilter(Pipe("僮쓉ᅡ⠭尔厅粑莹්犌"), Pipe("a"), List(TriplePattern(Value("subject"), PositionalField(1),
          PositionalField(2)))),
        BGPFilter(Pipe("b"), Pipe("僮쓉ᅡ⠭尔厅粑莹්犌"),
          List(TriplePattern(PositionalField(0), PositionalField(1), Value("object"))))),
      // subject & object bound, grouped by object
      (TriplePattern(Value("subject"), PositionalField(1), Value("object")),
        "object",
        BGPFilter(Pipe("ᚩ룰宿希᭤㕗鋅᥆䢩旚"), Pipe("a"), List(TriplePattern(PositionalField(0), PositionalField(1), Value
          ("object")))),
        BGPFilter(Pipe("b"), Pipe("ᚩ룰宿希᭤㕗鋅᥆䢩旚"),
          List(TriplePattern(Value("subject"), PositionalField(1), PositionalField(2))))),
      // predicate & object bound, grouped by predicate
      (TriplePattern(PositionalField(0), Value("predicate"), Value("object")),
        "predicate",
        BGPFilter(Pipe("缌㓀⼲킃㌶㾬䢴憩᥷ᣓ"), Pipe("a"), List(TriplePattern(PositionalField(0), Value("predicate"),
          PositionalField(2)))),
        BGPFilter(Pipe("b"), Pipe("缌㓀⼲킃㌶㾬䢴憩᥷ᣓ"),
          List(TriplePattern(PositionalField(0), PositionalField(1), Value("object"))))),
      // predicate & object bound, grouped by object
      (TriplePattern(PositionalField(0), Value("predicate"), Value("object")),
        "object",
        BGPFilter(Pipe("ᥴ倓✴톍눜槥呩ᱷ䧉⮨"), Pipe("a"),
          List(TriplePattern(PositionalField(0), PositionalField(1), Value("object")))),
        BGPFilter(Pipe("b"), Pipe("ᥴ倓✴톍눜槥呩ᱷ䧉⮨"),
          List(TriplePattern(PositionalField(0), Value("predicate"), PositionalField(2)))))
    )

    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F7))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only f1
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only f2
    }

    // Don't apply F7 to non-grouped data
    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F7))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }

    // Don't apply F7 if there's more than one pattern
    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p, p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F7))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }

    // Don't apply F7 if there are no patterns
    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List.empty)
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F7))
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
          val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F7))

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
    val pattern = TriplePattern(Value("subject"),Value("predicate"), Value("object"))
    val patterns = Table(
      ("Pattern", "grouping column", "Grouping column Filter", "Other Filter"),
      (pattern,
        "subject",
        BGPFilter(Pipe("–깍窶䠃ऑ졮硔镔鰛끺"), Pipe("a"), List(TriplePattern(Value("subject"), PositionalField(1),
          PositionalField(2)))),
        BGPFilter(Pipe("b"), Pipe("–깍窶䠃ऑ졮硔镔鰛끺"), List(TriplePattern(PositionalField(0), Value("predicate"),
          Value("object"))))),
      (pattern,
        "predicate",
        BGPFilter(Pipe("⛛⠥톦럫㓚喬詧ǌꏚ蟃"), Pipe("a"), List(TriplePattern(PositionalField(0), Value("predicate"),
          PositionalField(2)))),
        BGPFilter(Pipe("b"), Pipe("⛛⠥톦럫㓚喬詧ǌꏚ蟃"), List(TriplePattern(Value("subject"), PositionalField(1), Value
          ("object"))))),
      (pattern,
        "object",
        BGPFilter(Pipe("僮쓉ᅡ⠭尔厅粑莹්犌"), Pipe("a"), List(TriplePattern(PositionalField(0), PositionalField(1), Value
          ("object")))),
        BGPFilter(Pipe("b"), Pipe("僮쓉ᅡ⠭尔厅粑莹්犌"), List(TriplePattern(Value("subject"), Value("predicate"),
          PositionalField(2))))))

    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F8))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only f1
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only f2
    }

    // Don't apply F8 to non-grouped data
    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), None)
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F8))
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
        val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F8))
        plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
        plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
      }
    }

    // Don't apply F8 if there's more than one pattern
    forAll (patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List(p, p))
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F8))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }

    // Don't apply F8 if there are no patterns
    forAll(patterns) { (p: TriplePattern, g: String, f1: BGPFilter, f2: BGPFilter) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List.empty)
      val op3 = Dump(Pipe("b"))
      val plan = processPlan(new DataflowPlan(List(op1, op2, op3)), buildOperatorReplacementStrategy(Rules.F8))
      plan.sourceNodes.headOption.value.outputs.flatMap(_.consumer) should contain only op2
      plan.sinkNodes.headOption.value.inputs.map(_.producer) should contain only op2
    }
  }
}
