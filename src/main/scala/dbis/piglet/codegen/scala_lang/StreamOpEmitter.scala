package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.expr.Ref
import dbis.piglet.op.StreamOp

/**
  * Created by kai on 01.12.16.
  */
class StreamOpEmitter extends CodeEmitter[StreamOp] {
  override def template: String = """        val <in>_helper = <in>.map(t => List(<in_fields>))
                                    |        val <out> = <op>(sc, <in>_helper<params>).map(t => <class>(<out_fields>))
                                    |""".stripMargin

  override def code(ctx: CodeGenContext, op: StreamOp): String = {
    if(op.schema.isEmpty) {
      throw CodeGenException("Schema must be set for STREAM THROUGH operator")
    }

    val className = ScalaEmitter.schemaClassName(op.schema.get.className)

    val inFields = op.inputSchema.get.fields.zipWithIndex.map{ case (f, i) => s"t._$i"}.mkString(", ")
    val outFields = op.schema.get.fields.zipWithIndex.map{ case (f, i) => s"t($i).asInstanceOf[${ScalaEmitter.scalaTypeMappingTable(f.fType)}]"}.mkString(", ")

    render(Map("out" -> op.outPipeName,
        "op" -> op.opName,
        "in" -> op.inPipeName,
        "class" -> className,
        "in_fields" -> inFields,
        "out_fields" -> outFields,
        "params" -> ScalaEmitter.emitParamList(CodeGenContext(ctx, Map("schema" -> op.schema)), op.params)))
  }
}

object StreamOpEmitter {
	lazy val instance = new StreamOpEmitter
}