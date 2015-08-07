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

import dbis.pig.schema.{Schema, TupleType, BagType}
import dbis.test.TestTools._

import dbis.pig.PigCompiler._
import dbis.pig.op._
import dbis.pig.plan.DataflowPlan
import dbis.pig.plan.rewriting.Rewriter._
import dbis.pig.schema._
import org.scalatest.OptionValues._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class RewriterSpec extends FlatSpec with Matchers with TableDrivenPropertyChecks {
  "The rewriter" should "merge two Filter operations" in {
    val op1 = Load(Pipe("a"), "file.csv")
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
    val op1 = Load(Pipe("a"), "file.csv")
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
    val op1 = Load(Pipe("a"), "file.csv", Some(Schema(BagType(TupleType(Array(Field("a", Types.IntType), Field("aid", Types.IntType)))
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
    val op1 = Load(Pipe("a"), "file.csv")
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
    val op1 = Load(Pipe("a"), "file.csv")
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
    val op1 = Load(Pipe("a"), "file.csv")
    val plan = new DataflowPlan(List(op1))
    val newPlan = processPlan(plan)

    newPlan.sourceNodes shouldBe empty
  }

  it should "remove sink nodes that don't store a relation and have an empty outputs list" in {
    val op1 = Load(Pipe("a"), "file.csv")
    val plan = new DataflowPlan(List(op1))

    op1.outputs = List.empty
    val newPlan = processPlan(plan)

    newPlan.sourceNodes shouldBe empty
  }

  it should "pull up Empty nodes" in {
    val op1 = Load(Pipe("a"), "file.csv")
    val op2 = OrderBy(Pipe("b"), Pipe("a"), List())
    val plan = new DataflowPlan(List(op1, op2))
    plan.operators should have length 2

    val newPlan = processPlan(plan)

    newPlan.sourceNodes shouldBe empty
    newPlan.operators shouldBe empty
  }

  it should "apply rewriting rule R1" in {
    val op1 = RDFLoad(Pipe("a"), new URI("http://example.com"), None)
    val op2 = Dump(Pipe("a"))
    val plan = processPlan(new DataflowPlan(List(op1, op2)))
    val source = plan.sourceNodes.headOption.value
    source shouldBe Load(Pipe("a"), "http://example.com", op1.schema, "pig.SPARQLLoader",
      List("SELECT * WHERE { ?s ?p ?o }"))
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
}
