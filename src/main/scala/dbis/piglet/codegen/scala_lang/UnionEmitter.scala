package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{Union, PigOperator}

/**
  * Created by kai on 03.12.16.
  */
class UnionEmitter extends CodeEmitter {
  override def template: String = """val <out> = <in><others:{ e | .union(<e>)}>""".stripMargin


  override def code(ctx: CodeGenContext, node: PigOperator): String = {
    node match {
      case Union(out, rels) =>
        render(Map("out" -> node.outPipeName,
          "in" -> node.inPipeName,
          "others" -> node.inPipeNames.tail))
      case _ => throw CodeGenException(s"unexpected operator: $node")
    }
  }

}
