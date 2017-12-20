package dbis.piglet.mm

import java.net.URI
import java.nio.file.{Files, StandardOpenOption}

import dbis.piglet.Piglet.Lineage
import dbis.piglet.tools.logging.PigletLogging
import dbis.piglet.tools.{CliParams, Conf}
import org.json4s.NoTypeHints
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.io.Source



object EvictionStrategy extends Enumeration {
  type EvictionStrategy = Value

  val NONE, LRU, KNAPSACK = Value

  def getStrategy(v: EvictionStrategy): IEvictionStrategy = v match {
    case NONE => NoEviction
    case LRU => LRUEviction
    case KNAPSACK => KnapSackEviction
  }
}

trait IEvictionStrategy {
  val CACHE_SIZE = CliParams.values.profiling.get.cacheSize
  def apply(entries: Seq[CacheEntry]): Seq[CacheEntry]
}

object NoEviction extends IEvictionStrategy {
  override def apply(entries: Seq[CacheEntry]): Seq[CacheEntry] = entries
}

object LRUEviction extends IEvictionStrategy {
  def apply(entries: Seq[CacheEntry]): Seq[CacheEntry] = {
    var totalSize: Long = 0L
    val res = entries.filter(e => !e.fixed && e.lastLoaded.isDefined).sortBy(_.lastLoaded.get)(Ordering[Long].reverse).takeWhile{e =>
      val fitsInCache = totalSize + e.bytes < CACHE_SIZE
      if(fitsInCache)
        totalSize += e.bytes

      fitsInCache
    }

    // fixed results will be kept in cache - regardless of its size
    entries.filter(_.fixed) ++ res
  }
}

object KnapSackEviction extends IEvictionStrategy {

  override def apply(entries: Seq[CacheEntry]): Seq[CacheEntry] = {
    var totalSize: Long = 0L

    val res = entries.filter(e => !e.fixed ).sortBy(_.benefit)(Ordering[Duration].reverse).takeWhile{e =>
      val fitsInCache = totalSize + e.bytes < CACHE_SIZE
      if(fitsInCache)
        totalSize += e.bytes

      fitsInCache
    }

    entries.filter(_.fixed) ++ res

  }
}

object CacheManager extends PigletLogging {

  implicit val formats = Serialization.formats(NoTypeHints)

  /**
    * Already existing materializations
    *
    * They're read from file and stored as a mapping from lineage --> file name
    */
  var materializations: Map[Lineage, CacheEntry] = if (Files.exists(Conf.materializationMapFile)) {
    val json = Source.fromFile(Conf.materializationMapFile.toFile).getLines().mkString("\n")

    if(json.isEmpty)
      Map.empty[Lineage, CacheEntry]
    else
      parse(json).extract[Map[Lineage, CacheEntry]] //.map{case(k,v) => (k,new URI(v))}

  } else {
    Map.empty[Lineage, CacheEntry]
  }

  /**
    * Checks if we have materialized results for the given hash value
    *
    * @param lineage The hash value to get data for
    * @return Returns the path to the materialized result, iff present. Otherwise <code>null</code>
    */
  def getDataFor(lineage: Lineage): Option[URI] = {
    markLoaded(lineage)
    materializations.get(lineage).map(s => new URI(s.uri))
  }

  /**
    * Persist the given mapping of a materialization point to a specific file name,
    * according to a eviction strategy
    *
    * @param m       The materialization point
    * @param matFile The path to the file in which the results were materialized
    */
  def insert(m: MaterializationPoint, matFile: URI): Boolean = {

    require(!CliParams.values.compileOnly, "writing materialization mapping info in compile-only mode will break things!")

    val ps = CliParams.values.profiling.get
    val evictionStrategy = EvictionStrategy.getStrategy(ps.eviction)

    val entry = CacheEntry(m.lineage, matFile.toString, benefit = m.benefit, bytes = m.bytes, lastLoaded = Some(System.currentTimeMillis()))

    logger.info(s"using cache eviction strategy: $evictionStrategy")
    logger.info(s"max cache size is ${evictionStrategy.CACHE_SIZE}")

    val before = materializations.values.toSeq :+ entry
    logger.debug(s"cache before: \n ${before.mkString("\n")}")
    val remainingCacheContent = evictionStrategy(before)

    logger.debug(s"cache after: \n ${remainingCacheContent.mkString("\n")}")

//    materializations += m.lineage -> matFile

    materializations = remainingCacheContent.map{ e => e.lineage -> e}.toMap

    val json = write(materializations)

    Files.write(Conf.materializationMapFile,
      List(json).asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)


    val inserted = remainingCacheContent.exists(_.lineage == m.lineage)
    if(inserted)
      markWritten(m.lineage)

    inserted

  }

  def insert(sig: Lineage, file: URI): Boolean = {
    val entry = CacheEntry(sig, file.toString, benefit = Duration.Undefined, bytes = -1, written = Some(System.currentTimeMillis()), fixed = true)

    materializations += sig -> entry

    val json = write(materializations)

    Files.write(Conf.materializationMapFile,
      List(json).asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)

    true
  }



  private def markWritten(lineage: Lineage): Unit = materializations.find(_._1 == lineage)
                                                .foreach(_._2.written = Some(System.currentTimeMillis()))


  private def markLoaded(lineage: Lineage): Unit = materializations.find(_._1 == lineage).foreach{e =>
      e._2.lastLoaded = Some(System.currentTimeMillis())
      e._2.useCount += 1
    }

}

case class CacheEntry(lineage: Lineage, uri: String, benefit: Duration, bytes: Long, var lastLoaded: Option[Long] = None, var written: Option[Long] = None,
                      var useCount: Int = 0, var fixed: Boolean = false) {


  override def toString =
    s"""CacheEntry
       |  lineage: $lineage
       |  file: $uri
       |  benefit: ${benefit.toSeconds}
       |  bytes: $bytes
       |  lastLoaded: ${lastLoaded.getOrElse("-")}
       |  written: ${written.getOrElse("-")}
       |  use count: $useCount
       |  fixed: $fixed
     """.stripMargin
}