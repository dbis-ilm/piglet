package dbis.test.pig


/**
 * Created by kai on 01.04.15.
 */

import dbis.pig._
import org.scalatest.FlatSpec

class CompileSpec extends FlatSpec {
  "The compiler output" should "contain code for LOAD" in {
    val op1 = Load("a", "file.csv")
    val op2 = Dump("a")
    val plan = new DataflowPlan(List(op1, op2))
    val compiler = new SparkCompile
    val result = compiler.compile(plan)
    println(result)
  }

}
