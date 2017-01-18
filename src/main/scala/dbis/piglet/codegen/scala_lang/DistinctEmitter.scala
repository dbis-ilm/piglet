package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{Distinct, PigOperator}

/**
  * Created by kai on 03.12.16.
  */
class DistinctEmitter extends CodeEmitter[Distinct] {
  override def template: String = """val <out> = <in>.distinct""".stripMargin


  override def code(ctx: CodeGenContext, op: Distinct): String = 
        render(Map("out" -> op.outPipeName, "in" -> op.inPipeName))

}

object DistinctEmitter {
  lazy val instance = new DistinctEmitter
}