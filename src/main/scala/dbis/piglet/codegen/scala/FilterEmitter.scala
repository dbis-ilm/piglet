package dbis.piglet.codegen.scala

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{Filter, PigOperator}

/**
  * Created by kai on 01.12.16.
  */
class FilterEmitter extends CodeEmitter {
  override def template = """filter(out, in, pred) ::=<<
                            |        val <out> = <in>.filter(t => {<pred>})
                            |>>
                            |"""


  override def code(ctx: CodeGenContext, node: PigOperator) = {
    node match {
      case Filter(_, _, pred, _) =>
        render(Map("out" -> node.outPipeName,
          "in" -> node.inPipeName,
          "pred" -> ScalaEmitter.emitPredicate(CodeGenContext(ctx, Map[String,Any]("schema" -> node.schema)), pred)))
      case _ => throw new CodeGenException(s"unexpected operator: $node")
    }
  }
}
