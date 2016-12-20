package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.schema.SchemaException
import dbis.piglet.op.StreamOp
import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.expr.Ref

class StreamOpEmitter extends CodeEmitter[StreamOp] {
  override def template: String = """    val <in>_helper = <in>.map(t => List(<in_fields>))
                                    |    val <out> = <op>(sc, <in>_helper<params>).map(t => <class>(<out_fields>))""".stripMargin

  override def code(ctx: CodeGenContext, op: StreamOp): String = {
    // TODO: how to handle cases where no schema was given??
    if (op.schema.isEmpty) throw new SchemaException("Schema must be set for STREAM THROUGH operator")

    val className = ScalaEmitter.schemaClassName(op.schema.get.className)
    val inFields = op.inputSchema.get.fields.zipWithIndex.map { case (f, i) => s"t._$i" }.mkString(", ")
    val outFields = op.schema.get.fields.zipWithIndex.map { case (f, i) => s"t($i).asInstanceOf[${ScalaEmitter.scalaTypeMappingTable(f.fType)}]" }.mkString(", ")

    render(Map("out" -> op.outPipeName, "op" -> op.opName, "in" -> op.inPipeName, "class" -> className,
      "in_fields" -> inFields, "out_fields" -> outFields, "params" -> emitParamList(CodeGenContext(ctx, Map("schema" -> op.schema)), op.params)))
  }
  
  /**
   *
   * @param ctx an object representing context information for code generation
   * @param params the list of parameters (as Refs)
   * @return the generated code
   */
  def emitParamList(ctx: CodeGenContext, params: Option[List[Ref]]): String = params match {
    case Some(refList) => if (refList.nonEmpty) s",${refList.map(r => ScalaEmitter.emitRef(ctx, r)).mkString(",")}" else ""
    case None => ""
  }
}
