package dbis.piglet.codegen.spark

import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.op.SpatialJoin
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.schema.SchemaException
import dbis.piglet.codegen.scala_lang.ScalaEmitter


class SpatialJoinEmitter extends CodeEmitter[SpatialJoin] {
  
  override def template = s"""val <out> = <rel1>.join(
                    |   <rel2>.keyBy(<tuplePrefix> => <rightfield>),
                    |   dbis.stark.spatial.Predicates.<predicate> _
                    | ).map{ case (v,w) => 
                    |     <className>(<fields>) 
                    |\\}""".stripMargin
  
  def code(ctx: CodeGenContext, op: SpatialJoin): String = {
    
    if(op.schema.isEmpty)
      throw new SchemaException("Schema must be defiend for spatial join operator")
    
    
    val vsize = op.inputs.head.inputSchema.get.fields.length // number of fields in left relation
    val fieldList = op.schema.get.fields.zipWithIndex // all fields
        .map { case (f, i) => if (i < vsize) s"v._$i" else s"w._${i - vsize}" }.mkString(", ")
    
        
    val params = Map(
      "out" -> op.outPipeName,
      "className" -> ScalaEmitter.schemaClassName(op.schema.get.className),
      "tuplePrefix" -> ctx.asString("tuplePrefix"),
      "fields" -> fieldList,
      "predicate" -> op.predicate.predicateType.toString().toLowerCase(),
      "rel2" -> op.inPipeNames(1),
      "rightfield" -> ScalaEmitter.emitRef(CodeGenContext(ctx,Map("schema"->op.inputs(1).inputSchema)), op.predicate.right),
      "rel1" -> {if(op.withIndex) {
                  op.inPipeNames(0)
                } else {
                  s"${op.inPipeNames(0)}.keyBy(${ctx.asString("tuplePrefix")} => ${ScalaEmitter.emitRef(CodeGenContext(ctx,Map("schema"->op.inputs(0).inputSchema)), op.predicate.left)})"
                }}
    )

    render(params)
  }
}