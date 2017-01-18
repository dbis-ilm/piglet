package dbis.piglet.codegen.spark

import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.op.SpatialJoin
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.schema.SchemaException
import dbis.piglet.codegen.scala_lang.ScalaEmitter
import dbis.piglet.op.IndexMethod.IndexMethod


class SpatialJoinEmitter extends CodeEmitter[SpatialJoin] {
  
  override def template = s"""val <out> = <rel1><keyby1><liveindex>.join(
                    |   <rel2><keyby2>,
                    |   dbis.stark.spatial.JoinPredicate.<predicate>
                    | ).map{ case (v,w) => 
                    |     <className>(<fields>) 
                    |\\}""".stripMargin
  
  def indexTemplate(idxConfig: Option[(IndexMethod, List[String])]) = idxConfig match {
    case Some((indexMethod, params)) => CodeEmitter.render(".liveIndex(<params>)", Map("params" -> params.mkString(",")))
    case None => ""
  }                    
                    
  def code(ctx: CodeGenContext, op: SpatialJoin): String = {
    
    if(op.schema.isEmpty)
      throw new SchemaException("Schema must be defiend for spatial join operator")
    
    
    val vsize = op.inputs.head.inputSchema.get.fields.length // number of fields in left relation
    val fieldList = op.schema.get.fields.zipWithIndex // all fields
        .map { case (f, i) => if (i < vsize) s"v._$i" else s"w._${i - vsize}" }.mkString(", ")
    
        
    val params = Map(
      "out" -> op.outPipeName,
      "className" -> ScalaEmitter.schemaClassName(op.schema.get.className),
      "fields" -> fieldList,
      "predicate" -> op.predicate.predicateType.toString().toUpperCase(),
      "rel1" -> op.inPipeNames(0),
      "rel2" -> op.inPipeNames(1),
      "keyby1" -> {if(SpatialEmitterHelper.geomIsFirstPos(op.predicate.left, op)) "" 
                   else s".keyBy(${ctx.asString("tuplePrefix")} => ${ScalaEmitter.emitRef(CodeGenContext(ctx,Map("schema"->op.inputs(0).inputSchema)), op.predicate.left)})"},
      "keyby2" -> {if(SpatialEmitterHelper.geomIsFirstPos(op.predicate.right, op)) "" 
                   else s".keyBy(${ctx.asString("tuplePrefix")} => ${ScalaEmitter.emitRef(CodeGenContext(ctx,Map("schema"->op.inputs(1).inputSchema)), op.predicate.right)})"},
      "liveindex" -> indexTemplate(op.index)
    )

    render(params)
  }
}

object SpatialJoinEmitter {
	lazy val instance = new SpatialJoinEmitter
}