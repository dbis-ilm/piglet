package dbis.piglet.codegen.spark

import dbis.piglet.op.PigOperator
import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.expr.NamedField
import dbis.piglet.expr.PositionalField
import dbis.piglet.expr.Ref

object SpatialEmitterHelper {
  
  
  def geomIsFirstPos[T <: PigOperator](ref: Ref, op: T): Boolean = {
 
    val pos = ref match {
      case nf : NamedField =>
        op.inputSchema.get.indexOfField(nf)
      case pf : PositionalField =>
        pf.pos
      case _ => throw new IllegalArgumentException(s"expected field reference, got: ${ref}")
    }
    
    pos == 0
  }
  
}