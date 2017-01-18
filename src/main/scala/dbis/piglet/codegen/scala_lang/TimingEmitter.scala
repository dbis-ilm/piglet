package dbis.piglet.codegen.scala_lang

import dbis.piglet.op.TimingOp
import dbis.piglet.op.PigOperator
import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.codegen.CodeGenContext

class TimingEmitter extends CodeEmitter[TimingOp] {
  override def template = """val <out> = <in>.mapPartitionsWithIndex{ case (idx,iter) => 
    |  val jsonString = s"{'partitionId':'$idx','time':'${System.currentTimeMillis}','lineage':'<lineage>'}" 
    |  val result = scalaj.http.Http(url).postData(jsonString).header("Content-Type", "application/json").header("Charset","UTF-8").asString
    |  iter
    \}""".stripMargin
  
  override def code(ctx: CodeGenContext, op: TimingOp): String = render(Map(
      "out"-> op.outPipeName, 
      "in" -> op.inPipeName,
      "lineage" -> op.operatorId))
}