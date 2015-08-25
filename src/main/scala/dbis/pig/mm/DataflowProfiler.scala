package dbis.pig.mm

import scala.collection.mutable.Map

/**
 * Created by kai on 24.08.15.
 */
class DataflowProfiler {
  private val cache: Map[String, MaterializationPoint] = Map()

  def getMaterializationPoint(hash: String): Option[MaterializationPoint] = cache.get(hash)

  def addMaterializationPoint(matPoint: MaterializationPoint): Unit = {
    val entry = cache.getOrElse(matPoint.hash, matPoint)
    if (entry.parentHash.nonEmpty && cache.contains(entry.parentHash.get)) {
      // TODO: calculate _cumulative_ benefit
      val parent = cache.get(entry.parentHash.get).get
      val benefit = parent.loadTime + entry.procTime - entry.loadTime
      entry.benefit = parent.benefit + benefit
    }
    entry.count += 1
    cache += (matPoint.hash -> entry)
  }

  override def toString = cache.mkString(",")
}
