package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.spark.SpatialEmitterHelper
import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.Difference

class DifferenceEmitter extends CodeEmitter[Difference] {
  override def template: String = """val <out> = <in1>.subtract(<in2>)""".stripMargin

  def templateKeyed = """val <out> = <in1><keyby1>.subtractByKey(<in2><keyby2>).map(_._2)"""

  override def code(ctx: CodeGenContext, op: Difference): String = {

    val (templ,params) = if(op.refs1.isDefined) {
      val m = Map("out" -> op.outPipeName,
        "in1" -> op.inPipeNames.head,
        "in2" -> op.inPipeNames.last,
        "keyby1" -> SpatialEmitterHelper.keyByCode(op.inputs.head.producer.schema, op.refs1.get,ctx),
        "keyby2" -> SpatialEmitterHelper.keyByCode(op.inputs.last.producer.schema, op.refs2.get,ctx)
      )
      (templateKeyed, m)
    } else {
      val m = Map("out" -> op.outPipeName,
        "in1" -> op.inPipeNames.head,
        "in2" -> op.inPipeNames.last
      )
      (template, m)
    }

    CodeEmitter.render(templ, params)
  }

}

object DifferenceEmitter {
  lazy val instance = new DifferenceEmitter
}