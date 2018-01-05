package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.spark.SpatialEmitterHelper
import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.op.GroupingExpression
import dbis.piglet.op.cmd.CoGroup

class CoGroupEmitter extends CodeEmitter[CoGroup] {



  override def template =
    """val <out> = <in>.cogroup(<others>).map{case (k,(<vs>)) =>
      | val t = <class>(k,<vs>)
      | <if(profiling)>
      |   PerfMonitor.sampleSize(Seq(<vs>).flatten, "<lineage>", accum, randFactor)
      | <endif>
      | t
      |}
    """.stripMargin

  override def code(ctx: CodeGenContext, op: CoGroup) = {

    val className = ScalaEmitter.schemaClassName(op.schema.get.className)

    val keyBys = op.inputs.zip(op.groupExprs).map {

      case (pipe, GroupingExpression(List())) => // all
        pipe.name + """.keyBy(t => "all")"""

      case (pipe, GroupingExpression(List(ref))) =>
        pipe.name + SpatialEmitterHelper.keyByCode(pipe.producer.schema, ref, ctx)

      case (pipe, groupExpr@GroupingExpression(_ :: _)) => // at least two
        // (actually could be one,because the tail also matches empty list,
        // but this case was handled before)
        throw CodeGenException("multiple cogroup fields are not supported yet in Code generator")
//        ScalaEmitter.emitGroupExpr(CodeGenContext(ctx, Map("schema" -> op.inputSchema)), groupExpr)
    }

    val params = Map(
      "out" -> op.outPipeName,
      "in" -> keyBys.head,
      "others" -> keyBys.tail.mkString(","),
      "class" -> className,
      "vs" -> keyBys.indices.map(i => s"v$i").mkString(","),
      "lineage" -> op.lineageSignature
    )

    render(params)
  }

}


object CoGroupEmitter {
  lazy val instance = new CoGroupEmitter
}
