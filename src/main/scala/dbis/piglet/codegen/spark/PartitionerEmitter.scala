package dbis.piglet.codegen.spark

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.{Partition, PartitionMethod}

class PartitionerEmitter extends CodeEmitter[Partition]  {

  private val partitionerTemplate = "new <method>(<params>)"
  private val spatialPartitionerTemplate = "new <method>(<in>_helper,<params>)"

  override def template = """val <out> = {
         |  val <in>_helper = <in><keyby> 
         |  <in>_helper.partitionBy(<partitioner>).map{case (_,v)=>
         |    <if (profiling)>
         |    if(scala.util.Random.nextInt(randFactor) == 0) {
         |      accum.incr("<lineage>", v)
         |    }
         |    <endif>
         |
         |    v
         |  \}
         |\}""".stripMargin
  
  override def code(ctx: CodeGenContext, op: Partition): String = {


    val partitioner = {

      val (template, methodClass) = op.method match {
        case PartitionMethod.GRID => (spatialPartitionerTemplate, "SpatialGridPartitioner")
        case PartitionMethod.BSP => (spatialPartitionerTemplate, "BSPartitioner")
        case PartitionMethod.Hash => (partitionerTemplate, "org.apache.spark.HashPartitioner")
      }

      CodeEmitter.render(template, Map(
        "method" -> methodClass,
        "params" -> op.params.mkString(",")))

    }

    render(Map(
      "out" -> op.outPipeName,
      "in" -> op.inPipeName,
      "partitioner" -> partitioner,
      "keyby" -> SpatialEmitterHelper.keyByCode(op.inputSchema, op.field, ctx),
      "lineage" -> op.lineageSignature
		))
    
  }
}

object PartitionerEmitter {
	lazy val instance = new PartitionerEmitter
}