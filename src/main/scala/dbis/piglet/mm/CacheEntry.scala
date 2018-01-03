package dbis.piglet.mm

import dbis.piglet.Piglet.Lineage

import scala.concurrent.duration._

case class CacheEntry(lineage: Lineage, uri: String, _benefit: Long, bytes: Long, var lastLoaded: Option[Long] = None, var written: Option[Long] = None,
                      var useCount: Int = 0, var fixed: Boolean = false) {


  def benefit: Duration = _benefit.milliseconds

  def markWritten() = written = Some(System.currentTimeMillis())

  def markLoaded() = {
    lastLoaded = Some(System.currentTimeMillis())
    useCount += 1
  }

  override def toString =
    s"""CacheEntry
       |  lineage: $lineage   file: $uri   benefit: ${benefit.toSeconds} (${_benefit} ms)   bytes: $bytes    lastLoaded: ${lastLoaded.getOrElse("-")}   written: ${written.getOrElse("-")}
       |     use count: $useCount    fixed: $fixed""".stripMargin

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: CacheEntry =>
      o.lineage equals lineage
    case _ => false
  }

  override def hashCode(): Int = lineage.hashCode
}
