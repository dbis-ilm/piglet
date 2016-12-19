package dbis.piglet.codegen.spark

import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.op.SpatialJoin
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.schema.SchemaException
import dbis.piglet.codegen.scala_lang.ScalaEmitter


class SpatialJoinEmitter extends CodeEmitter[SpatialJoin] {
  
  override def template = s"""val <out> = <rel1>.join(<rel2>,dbis.stark.spatial.Predicates.<predicate> _).map{ case (v,w) => <className>(<fields>) \\}""".stripMargin
  
  def code(ctx: CodeGenContext, op: SpatialJoin): String = {
    
    if(op.schema.isEmpty)
      throw new SchemaException("Schema must be defiend for spatial join operator")
    
    
    val vsize = op.inputs.head.inputSchema.get.fields.length
    val fieldList = op.schema.get.fields.zipWithIndex
        .map { case (f, i) => if (i < vsize) s"v._$i" else s"w._${i - vsize}" }.mkString(", ")
    
    val params = Map(
      "out" -> op.outPipeName,
      "className" -> op.schema.get.className,
      "fields" -> fieldList,
      "predicate" -> op.predicate.predicateType.toString().toLowerCase(),
      "rel2" -> s"${op.inPipeNames(1)}.keyBy(t => ${ScalaEmitter.emitRef(ctx, op.predicate.right)})",
      "rel1" -> {if(op.withIndex) {
                  op.inPipeNames(0)
                } else {
                  s"${op.inPipeNames(0)}.keyBy(t => ${ScalaEmitter.emitRef(ctx, op.predicate.left)})"
                }}
    )

    val res = render(params)
    println(res)
    res        
  }
}