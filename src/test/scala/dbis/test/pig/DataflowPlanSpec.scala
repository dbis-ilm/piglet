package dbis.test.pig

/**
 * Created by kai on 01.04.15.
 */

import dbis.pig._
import org.scalatest.FlatSpec

class DataflowPlanSpec extends FlatSpec {
  "The plan" should "contain all pipes" in {
    val op1 = Load("a", "file.csv")
    val op2 = Foreach("b", "a", List("x", "y"))
    val op3 = Dump("b")
    val plan = new DataflowPlan(List(op1, op2, op3))
    assert(plan.pipes == Map("a" -> Pipe("a", op1), "b" -> Pipe("b", op2)))
  }

  it should "not contain duplicate pipes" in {
    val op1 = Load("a", "file.csv")
    val op2 = Foreach("b", "a", List("x", "y"))
    val op3 = Dump("b")
    val op4 = Foreach("b", "a", List("y", "z"))
    intercept[InvalidPlanException] {
      new DataflowPlan(List(op1, op2, op3, op4))
    }
  }

  it should "check connectivity" in {
    val op1 = Load("a", "file.csv")
    val op2 = Foreach("b", "a", List("x", "y"))
    val op3 = Dump("b")
    val op4 = Foreach("c", "a", List("y", "z"))
    val op5 = Dump("c")
    val plan = new DataflowPlan(List(op1, op2, op3, op4, op5))
    assert(plan.checkConnectivity)
  }

  it should "find disconnected operators" in {
    val op1 = Load("a", "file.csv")
    val op2 = Foreach("b", "a", List("x", "y"))
    val op3 = Dump("b")
    val op4 = Load("c", "file.csv")
    val op5 = Dump("c")
    val plan = new DataflowPlan(List(op1, op2, op3, op4, op5))
    assert(!plan.checkConnectivity)
  }

  it should "find sink operators" in {
    val op1 = Load("a", "file.csv")
    val op2 = Foreach("b", "a", List("x", "y"))
    val op3 = Dump("b")
    val op4 = Load("c", "file.csv")
    val op5 = Dump("c")
    val plan = new DataflowPlan(List(op1, op2, op3, op4, op5))
    assert(plan.sinkNodes == Set(op3, op5))
  }
}
