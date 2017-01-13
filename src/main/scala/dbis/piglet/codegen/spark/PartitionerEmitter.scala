package dbis.piglet.codegen.spark

import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.op.Partition
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.op.PartitionMethod
import dbis.piglet.codegen.scala_lang.ScalaEmitter

class PartitionerEmitter extends CodeEmitter[Partition]  {
  override def template = """val <out> = {
         |  val <in>_helper = <in><keyby> 
         |  <in>_helper.partitionBy(new <method>(<in>_helper,<params>)).map{case (_,v)=>v\}
         |\}""".stripMargin
  
  override def code(ctx: CodeGenContext, op: Partition): String = {
    val methodClass = op.method match {
        case PartitionMethod.GRID => "SpatialGridPartitioner"
        case PartitionMethod.BSP => "BSPartitioner"
      }
    
    render(Map(
      "out" -> op.outPipeName,
      "in" -> op.inPipeName,
      "method" -> methodClass,
      "params" -> op.params.mkString(",") ,
      "keyby" -> {if(SpatialEmitterHelper.geomIsFirstPos(op.field, op)) "" 
                  else s".keyBy(${ctx.asString("tuplePrefix")} => ${ScalaEmitter.emitRef(CodeGenContext(ctx,Map("schema"->op.inputSchema)), op.field)})"}
		))
    
  }
}