package dbis.piglet.codegen.spark

import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.op.{PartitionMethod, SpatialJoin}
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.schema.{IndexType, SchemaException, TupleType}
import dbis.piglet.codegen.scala_lang.ScalaEmitter
import dbis.piglet.op.IndexMethod.IndexMethod
import dbis.piglet.op.PartitionMethod.PartitionMethod


class SpatialJoinEmitter extends CodeEmitter[SpatialJoin] {
//  new dbis.stark.spatial.partitioner.BSPartitioner(<rel2>KeyBy, 1, 1000, true)
// val <rel2>Parti = new dbis.stark.spatial.partitioner.SpatialGridPartitioner(<rel2>KeyBy,20,true)  
  override def template = s"""
                     |val <rel2>KeyBy = <rel2><keyby2>
                     |val <rel1>KeyBy = <rel1><keyby1>
                     |<parti1>
                     |<parti2>
                     |val <out> = <rel1>KeyBy<if (parti1)>.partitionBy(<rel1>Parti)<endif><liveindex>.join(
                    |   <rel2>KeyBy<if (parti2)>.partitionBy(<rel2>Parti)<endif>,
                    |   dbis.stark.spatial.JoinPredicate.<predicate>
                    | ).map{ case (<leftName>,<rightName>) =>
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

  lazy val partitionTemplate =
    s"""
       |val <rel>Parti = new dbis.stark.spatial.partitioner.<methodclass>(<rel>KeyBy, <params>)
     """.stripMargin


  def indexTemplate(idxConfig: Option[(IndexMethod, List[String])]) = idxConfig match {
    case Some((_, params)) => CodeEmitter.render(".liveIndex(<params>)", Map("params" -> params.mkString(",")))
    case None => ""
  }                    
                    
  def code(ctx: CodeGenContext, op: SpatialJoin): String = {
    if(op.schema.isEmpty)
      throw SchemaException("Schema must be defiend for spatial join operator")
    
    
    val vsize = if(op.inputs.head.inputSchema.get.isIndexed) {
      op.inputs.head.inputSchema.get // schema of left (indexed) relation
        .element.valueType.asInstanceOf[IndexType] // contains a bag of Indexes
        .valueType.fields // An Index contains tuples with two fields: indexed column and payload
        .last.fType.asInstanceOf[TupleType] // payload is again a tuple
        .fields.length // number of fields in tuple - will be the fields in the result of the join
    } else
      op.inputs.head.inputSchema.get.fields.length // number of fields in left relation



//    val fieldList = op.schema.get.fields.zipWithIndex // all fields
//        .map { case (_, i) => if (i < vsize) s"v._$i" else s"w._${i - vsize}" }.mkString(", ")


    val lName = "v"
    val rName = "w"

    val fieldList = op.schema.get.fields.indices// all fields
            .map { i => if (i < vsize) s"$lName._$i" else s"$rName._${i - vsize}" }.mkString(", ")

    var params = Map(
      "out" -> op.outPipeName,
      "className" -> ScalaEmitter.schemaClassName(op.schema.get.className),
      "fields" -> fieldList,
      "predicate" -> op.predicate.predicateType.toString.toUpperCase(),
      "rel1" -> op.inPipeNames.head,
      "rel2" -> op.inPipeNames.last,
      "keyby1" -> {
        if(op.schema.nonEmpty && op.inputs.head.inputSchema.get.isIndexed)
          ""
        else
          SpatialEmitterHelper.keyByCode(op.inputs.head.inputSchema, op.predicate.left, ctx)
      },
      "keyby2" -> SpatialEmitterHelper.keyByCode(op.inputs.last.inputSchema, op.predicate.right, ctx),
//      "keyby1" -> s".keyBy(${ctx.asString("tuplePrefix")} => ${ScalaEmitter.emitRef(CodeGenContext(ctx,Map("schema"->op.inputs.head.inputSchema)), op.predicate.left)})",
//      "keyby2" -> s".keyBy(${ctx.asString("tuplePrefix")} => ${ScalaEmitter.emitRef(CodeGenContext(ctx,Map("schema"->op.inputs.last.inputSchema)), op.predicate.right)})",
      "liveindex" -> indexTemplate(op.index),
      "lineage" -> op.lineageSignature,
      "leftName" -> lName,
      "rightName" -> rName
    )

    op.leftParti.map{ case (method, mparams) => getPartitionerCode(method, mparams, op.inPipeNames.head) }.foreach{ parti => params += "parti1" -> parti}
    op.rightParti.map{ case (method, mparams) => getPartitionerCode(method, mparams,op.inPipeNames.last) }.foreach{ parti => params += "parti2" -> parti}


    render(params)
  }

  private def getPartitionerCode(method: PartitionMethod, params: List[String],relName: String): String = {
    val methodclass = method match {
      case PartitionMethod.GRID => "SpatialGridPartitioner"
      case PartitionMethod.BSP => "BSPartitioner"
      case _ => ???
    }

    val m = Map(
      "methodclass" -> methodclass,
      "params" -> params.mkString(","),
      "rel" -> relName
    )

    CodeEmitter.render(partitionTemplate, m)
  }
}

object SpatialJoinEmitter {
	lazy val instance = new SpatialJoinEmitter
}
