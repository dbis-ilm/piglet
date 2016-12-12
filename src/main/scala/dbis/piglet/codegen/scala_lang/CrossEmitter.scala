package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.{Cross, PigOperator}

/**
  * Created by kai on 12.12.16.
  */
class CrossEmitter extends CodeEmitter {
  override def template: String = """val <out> = <rel1><others:{e | .cartesian(<e>).map{ case (v,w)  => <class>(<fields>) \} }>""".stripMargin


  override def code(ctx: CodeGenContext, node: PigOperator): String = {
    node match {
      case Cross(_, _,  _) => {
        val rels = node.inputs

        val vsize = rels.head.inputSchema.get.fields.length
        val fieldList = node.schema.get.fields.zipWithIndex
          .map { case (f, i) => if (i < vsize) s"v._$i" else s"w._${i - vsize}" }.mkString(", ")

        val className = node.schema match {
          case Some(s) => ScalaEmitter.schemaClassName(s.className)
          case None => ScalaEmitter.schemaClassName(node.outPipeName)
        }
        render(Map(
          "out" -> node.outPipeName,
          "rel1" -> rels.head.name,
          "others" -> rels.tail.map(_.name),
          "class" -> className,
          "fields" -> fieldList))
      }
      case _ => throw CodeGenException(s"unexpected operator: $node")
    }
  }

}
