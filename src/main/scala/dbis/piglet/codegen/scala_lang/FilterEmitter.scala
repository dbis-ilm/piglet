package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.Filter

/**
  * Created by kai on 01.12.16.
  */
class FilterEmitter extends CodeEmitter[Filter] {
  override def template: String = """    val <out> = <in>.filter(t => {<pred>})""".stripMargin


  override def code(ctx: CodeGenContext, op: Filter): String = render(Map("out" -> op.outPipeName,
          "in" -> op.inPipeName,
          "pred" -> ScalaEmitter.emitPredicate(CodeGenContext(ctx, Map[String,Any]("schema" -> op.schema)), op.pred)))
}

object FilterEmitter {
  lazy val instance = new FilterEmitter
}