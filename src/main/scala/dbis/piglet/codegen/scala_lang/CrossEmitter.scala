package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.Cross

/**
  * Created by kai on 12.12.16.
  */
class CrossEmitter extends CodeEmitter[Cross] {
  override def template: String = """val <out> = <rel1><others:{e | .cartesian(<e>).map{ case (v,w)  => <class>(<fields>) \} }>""".stripMargin


  override def code(ctx: CodeGenContext, op: Cross): String = {
    val rels = op.inputs

    val vsize = rels.head.inputSchema.get.fields.length
    val fieldList = op.schema.get.fields.zipWithIndex
      .map { case (f, i) => if (i < vsize) s"v._$i" else s"w._${i - vsize}" }.mkString(", ")

    val className = op.schema match {
      case Some(s) => ScalaEmitter.schemaClassName(s.className)
      case None => ScalaEmitter.schemaClassName(op.outPipeName)
    }
    render(Map(
      "out" -> op.outPipeName,
      "rel1" -> rels.head.name,
      "others" -> rels.tail.map(_.name),
      "class" -> className,
      "fields" -> fieldList))
  }

}
