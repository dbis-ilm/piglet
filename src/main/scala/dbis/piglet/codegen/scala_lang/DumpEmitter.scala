package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{Dump, PigOperator}

/**
  * Created by kai on 05.12.16.
  */
class DumpEmitter extends CodeEmitter{
  override def template: String = """ <in>.collect.foreach(t => println(t.mkString()))""".stripMargin


  override def code(ctx: CodeGenContext, node: PigOperator): String = {
    node match {
      case Dump(in) => render(Map("in" -> node.inPipeName))
      case _ => throw CodeGenException(s"unexpected operator: $node")
    }
  }

}
