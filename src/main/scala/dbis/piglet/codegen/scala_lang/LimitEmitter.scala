package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{Limit, PigOperator}

/**
  * Created by kai on 03.12.16.
  */
class LimitEmitter extends CodeEmitter[Limit] {
  override def template: String = """val <out> = sc.parallelize(<in>.take(<num>))""".stripMargin


  override def code(ctx: CodeGenContext, op: Limit): String = 
        render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "num" -> op.num))

}
