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
                    |     val t = <className>(<fields>)
                    |     <if (profiling)>
                    |       PerfMonitor.sampleSize(t,"<lineage>", accum, randFactor)
                    |     <endif>
                    |     t
                    |\\}""".stripMargin

//  partitionBy(new dbis.stark.spatial.partitioner.SpatialGridPartitioner())



//  override def template = s"""val <rel1>_kvJoin = <rel1><keyby1>
//                             |val <rel1>_kvJoin_parted = <rel1>_kvJoin.partitionBy(new dbis.stark.spatial.partitioner.SpatialGridPartitioner(<rel1>_kvJoin, 6, pointsOnly=false,-180,180,-90,90,2))
//                             |val <rel2>_kvJoin = <rel2><keyby2>
//                             |val <rel2>_kvJoin_parted = <rel2>_kvJoin.partitionBy(new dbis.stark.spatial.partitioner.SpatialGridPartitioner(<rel2>_kvJoin, 6, pointsOnly=true,-180,180,-90,90,2))
//                             |
//                             |val <out> = <rel1>_kvJoin_parted<liveindex>.join(
//                             |   <rel2>_kvJoin_parted,
//                             |   dbis.stark.spatial.JoinPredicate.<predicate>
//                             | ).map{ case (v,w) =>
//                             |     <className>(<fields>)
//                             |\\}""".stripMargin
  
  def indexTemplate(idxConfig: Option[(IndexMethod, List[String])]) = idxConfig match {
    case Some((_, params)) => CodeEmitter.render(".liveIndex(<params>)", Map("params" -> params.mkString(",")))
    case None => ""
  }                    
                    
  def code(ctx: CodeGenContext, op: SpatialJoin): String = {
    if(op.schema.isEmpty)
      throw SchemaException("Schema must be defiend for spatial join operator")
    
    
    val vsize = op.inputs.head.inputSchema.get.fields.length // number of fields in left relation
    val fieldList = op.schema.get.fields.zipWithIndex // all fields
        .map { case (f, i) => if (i < vsize) s"v._$i" else s"w._${i - vsize}" }.mkString(", ")
    
        
    val params = Map(
      "out" -> op.outPipeName,
      "className" -> ScalaEmitter.schemaClassName(op.schema.get.className),
      "fields" -> fieldList,
      "predicate" -> op.predicate.predicateType.toString.toUpperCase(),
      "rel1" -> op.inPipeNames.head,
      "rel2" -> op.inPipeNames.last,
      "keyby1" -> SpatialEmitterHelper.keyByCode(op.inputs.head.inputSchema, op.predicate.left, ctx),
      "keyby2" -> SpatialEmitterHelper.keyByCode(op.inputs.last.inputSchema, op.predicate.right, ctx),
//      "keyby1" -> s".keyBy(${ctx.asString("tuplePrefix")} => ${ScalaEmitter.emitRef(CodeGenContext(ctx,Map("schema"->op.inputs.head.inputSchema)), op.predicate.left)})",
//      "keyby2" -> s".keyBy(${ctx.asString("tuplePrefix")} => ${ScalaEmitter.emitRef(CodeGenContext(ctx,Map("schema"->op.inputs.last.inputSchema)), op.predicate.right)})",
      "liveindex" -> indexTemplate(op.index),
      "lineage" -> op.lineageSignature
    )

    render(params)
  }
}

object SpatialJoinEmitter {
	lazy val instance = new SpatialJoinEmitter
}