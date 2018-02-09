package dbis.piglet.codegen.spark

import dbis.piglet.codegen.scala_lang.ScalaEmitter
import dbis.piglet.op.PigOperator
import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.expr.NamedField
import dbis.piglet.expr.PositionalField
import dbis.piglet.expr.Ref
import dbis.piglet.schema.Schema

object SpatialEmitterHelper {
  
  
  def geomIsFirstPos[T <: PigOperator](ref: Ref, op: T): Boolean = {
 
    val pos = ref match {
      case nf : NamedField =>
        op.inputSchema.get.indexOfField(nf)
      case pf : PositionalField =>
        pf.pos
      case _ => throw new IllegalArgumentException(s"expected field reference, got: $ref")
    }
    
    pos == 0
  }


  def keyByCode(schema: Option[Schema], ref: Ref, ctx: CodeGenContext): String =
    s".keyBy(${ctx.asString("tuplePrefix")} => ${ScalaEmitter.emitRef(CodeGenContext(ctx,Map("schema"->schema)), ref)})"


  def keyByCode(schema: Option[Schema], refs: Iterable[Ref], ctx: CodeGenContext): String =
    s".keyBy(${ctx.asString("tuplePrefix")} => (${refs.map(ref => ScalaEmitter.emitRef(CodeGenContext(ctx,Map("schema"->schema)), ref)).mkString(",")}))"
}