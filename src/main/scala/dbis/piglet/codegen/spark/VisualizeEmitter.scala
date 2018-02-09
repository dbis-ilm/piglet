package dbis.piglet.codegen.spark

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.Visualize

class VisualizeEmitter extends CodeEmitter[Visualize] {
  override def template: String = s"""<in><keyby>.visualize(<width>,<height>, "<path>", "<ext>"<pointsize>)"""

  override def code(ctx: CodeGenContext, op: Visualize): String = {
    val m = Map(
      "in" -> op.inPipeName,
      "width" -> op.width,
      "height" -> op.height,
      "path" -> op.pathNoExt,
      "ext" -> op.fileType,
      "keyby" -> SpatialEmitterHelper.keyByCode(op.schema,op.field, ctx),
      "pointsize" -> op.pointSize.map(p => s",pointSize = $p").getOrElse("")
    )

    render(m)
  }
}

object VisualizeEmitter {
  lazy val instance = new VisualizeEmitter
}
