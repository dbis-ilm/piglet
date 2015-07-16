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

import dbis.pig.op._
import dbis.pig.PigCompiler._
import dbis.pig.plan.DataflowPlan
import dbis.pig.plan.rewriting.Rewriter._
import org.scalatest.OptionValues._
import org.scalatest.{FlatSpec, Matchers}
import dbis.pig.PigCompiler._
import dbis.pig.plan.MaterializationManager
import java.io.File
import dbis.pig.plan.PrettyPrinter
import java.io.PrintWriter

class RewriterSpec extends FlatSpec with Matchers {
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
    rewrittenSink.outputs should equal(sourceMerged.outputs)

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

    // ops after reordering
    val op2_2 = Filter(Pipe("b"), Pipe("a"), predicate1)
    val op3_2 = OrderBy(Pipe("c"), Pipe("b"), List())
    val op4_2 = op4.copy()

    val plan = new DataflowPlan(List(op1, op2, op3, op4))
    val planReordered = new DataflowPlan(List(op1, op2_2, op3_2, op4_2))
    val source = plan.sourceNodes.head
    val sourceReordered = planReordered.sourceNodes.head

    val rewrittenSource = processPigOperator(source)
    rewrittenSource.outputs.head should equal(sourceReordered.outputs.head)

    val pPlan = processPlan(plan)
    pPlan.findOperatorForAlias("b").value should be(op2_2)
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

  it should "add store for (non existing) materialize results" in {
    val plan = new DataflowPlan(parseScript("""
         |a = load 'file.csv';
         |b = filter a by $0 > 0;
         |materialize b;
         |c = distinct b;
         |""".stripMargin))    
    
    val baseDir = new File("/tmp/piglet_test")
    val matFile = new File(baseDir, "mappings.txt")
    
    if(!baseDir.exists())
      baseDir.mkdirs()
      
    val p = new PrintWriter(matFile)      
    p.write("")
    p.close()
    
    val mm = new MaterializationManager(matFile, baseDir)
    
    val plan2 = processMaterializations(plan, mm)
    
    var matOp: Option[Store] = None
    
    for(op <- plan2.operators) {
      op match {
        case f : Store => matOp = Some(f) 
        case _ => 
      }
    }
    
//    plan.sinkNodes.map(PrettyPrinter.pretty).foreach(println)
    
    matOp shouldBe defined
    
  }
  
  it should "add LOAD for existing materialize results" in {
    val plan = new DataflowPlan(parseScript("""
         |a = load 'file.csv';
         |b = filter a by $0 > 0;
         |materialize b;
         |c = distinct b;
         |""".stripMargin))    
    
    val baseDir = new File("/tmp/piglet_test")
    val matFile = new File(baseDir, "mappings.txt")
    
    if(!baseDir.exists())
      baseDir.mkdirs()
      
    val mat = plan.findOperator { o => o.isInstanceOf[Materialize] }.head match {
      case o: Materialize => o
      case _ => throw new ClassCastException
    }  
      
    // empty the file
    val matResultFile = s"$baseDir/blubb.txt"
    val lineage = mat.lineageSignature 
    val p = new PrintWriter(matFile)      
    p.write(s"$lineage;$matResultFile\n")
    p.close()
    
    val mm = new MaterializationManager(matFile, baseDir)
    
    val plan2 = processMaterializations(plan, mm)
    
//    println(plan2.operators)
    
    var loadOp: Option[Load] = None
    
    /* FIXME: this finds the original load operator, because it was not 
     * removed from the plan
     * 
     * Adding the load just inserts a load and disconnects the subplans
     * However, the now unused subplan still exists and this test finds
     * the unused load operator... :(
     */
    for(op <- plan2.operators) {
      op match {
        case f : Load if f.file != "file.csv" => loadOp = Some(f) // bad workaround for the aforementioned issue 
        case _ => 
      }
    }
    
//    plan.sinkNodes.map(PrettyPrinter.pretty).foreach(println)
    
    withClue("load operator") {loadOp shouldBe defined}
    
    val op = loadOp.get
    
    withClue("loader file name: ") {op.file shouldBe matResultFile}
    
    withClue("loader inputs") {op.inputs shouldBe empty}
    
    val c = plan2.findOperatorForAlias("c").get
    withClue("load's successor's input size") {c.inputs.size shouldBe 1}
    withClue("load's successor's input op") {c.inputs(0) shouldBe Pipe("b", op)}
    
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
    rewrittenSource.outputs should contain only Pipe("a", op1, List(op2))
  }
}
