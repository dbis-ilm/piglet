package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.Intersection

class IntersectionEmitter extends CodeEmitter[Intersection] {
    override def template: String = """val <out> = <in1>.intersection(<in2>)""".stripMargin


    override def code(ctx: CodeGenContext, op: Intersection): String = render(Map("out" -> op.outPipeName,
      "in1" -> op.inPipeNames.head,
      "in2" -> op.inPipeNames.last
    ))

  }

  object IntersectionEmitter {
    lazy val instance = new IntersectionEmitter
  }