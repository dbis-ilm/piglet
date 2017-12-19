package dbis.piglet.mm

import dbis.piglet.Piglet.Lineage

import scala.concurrent.duration.Duration

/**
  * A MaterializationPoint object represents information about a possible materialization of the result
  * of a dataflow operator. It is identified by a hash of the lineage string of the operator and collects
  * profile information.
  *
  * @param lineage the MD5 hash of the lineage string of the operator
  * @param benefit the cumulative benefit of this materialization point compared to the root operator
  * @param prob The probability for re-using this operator
  * @param cost The duration that this operator takes
 */
case class MaterializationPoint(lineage: Lineage, prob: Double, cost: Long, benefit: Duration = Duration.Undefined) {
  override def hashCode(): Int = lineage.hashCode

  override def equals(obj: scala.Any): Boolean = obj match {
    case MaterializationPoint(l,_,_,_) => l equals lineage
    case _ => false
  }

  override def toString = s"MaterializationPoint($lineage, prob=$prob, cost=$cost ms, benefit=${benefit.toMillis} ms)"
}


object MaterializationPoint {
  def dummy(lineage: Lineage): MaterializationPoint = MaterializationPoint(lineage, -1,-1, Duration.Undefined)
}