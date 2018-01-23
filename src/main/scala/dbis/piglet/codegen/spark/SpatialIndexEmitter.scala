package dbis.piglet.codegen.spark

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.{IndexMethod, IndexOp}

class SpatialIndexEmitter extends CodeEmitter[IndexOp] {
//  new dbis.stark.spatial.partitioner.SpatialGridPartitioner(<in>KeyBy, partitionsPerDimension=20, pointsOnly=false)
  override def template =
    """val <in>KeyBy = <in><keyby>
      |val idxParti<out> = new dbis.stark.spatial.partitioner.BSPartitioner(<in>KeyBy, 1, 1000, false)
      |val <out> = <in>KeyBy.index(Some(idxParti<out>)<if (params)>, <params><endif>)<if (profiling)>.map{ idx =>
      |  PerfMonitor.sampleSize(idx, "<lineage>", accum, randFactor)
      |  idx
      |}<endif>""".stripMargin
  
  override def code(ctx: CodeGenContext, op: IndexOp): String = render(Map(
      "out" -> op.outPipeName,
      "in" -> op.inPipeName,
      "method" -> IndexMethod.methodName(op.method),
      "params" -> op.params.mkString(","),
      "keyby" -> SpatialEmitterHelper.keyByCode(op.inputSchema, op.field, ctx),
      "lineage" -> op.lineageSignature
    ) )
}

object SpatialIndexEmitter {
	lazy val instance = new SpatialIndexEmitter
}