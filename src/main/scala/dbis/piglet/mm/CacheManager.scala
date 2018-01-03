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
import scala.concurrent.duration._
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
  def wouldRemove(newEntry: CacheEntry, oldEntries: Seq[CacheEntry]): Seq[CacheEntry]
}

object NoEviction extends IEvictionStrategy {
  override def wouldRemove(newEntry: CacheEntry, oldEntries: Seq[CacheEntry]): Seq[CacheEntry] = Seq.empty
}

object LRUEviction extends IEvictionStrategy {

  def wouldRemove(newEntry: CacheEntry, oldEntries: Seq[CacheEntry]): Seq[CacheEntry] = {
    var totalSize: Long = newEntry.bytes

    val toProcess = oldEntries.filter(e => !e.fixed && e.lastLoaded.isDefined).sortBy(_.lastLoaded.get)(Ordering[Long].reverse)

//    println("to process")
//    toProcess.foreach(println)

    def fitsInCache(e: CacheEntry): Boolean = {
      totalSize + e.bytes < CACHE_SIZE
    }


    val res = toProcess.takeWhile{e =>
      val fitsInCache = totalSize + e.bytes < CACHE_SIZE
      if(fitsInCache)
        totalSize += e.bytes

      fitsInCache
    }.size

    // take the remaining elements
    val res2 = toProcess.slice(res, toProcess.size)
//    println("res")
//    res2.foreach(println)

    res2

    // fixed results will be kept in cache - regardless of its size
//    entries.filter(_.fixed) ++ res
  }
}

object KnapSackEviction extends IEvictionStrategy {

  override def wouldRemove(newEntry: CacheEntry, oldEntries: Seq[CacheEntry]): Seq[CacheEntry] = {
    var totalSize: Long = 0L

    val toProcess = (oldEntries :+ newEntry).filter(e => !e.fixed ).sortBy(_.benefit)(Ordering[Duration].reverse)

    val res = toProcess.takeWhile{e =>
      val fitsInCache = totalSize + e.bytes < CACHE_SIZE
      if(fitsInCache)
        totalSize += e.bytes

      fitsInCache
    }.size


    toProcess.slice(res, toProcess.size)

//    entries.filter(_.fixed) ++ res


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
    val e = materializations(lineage)
    e.markLoaded
    Some(new URI(e.uri))
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

    val entry = CacheEntry(m.lineage, matFile.toString, _benefit = m.benefit.toMillis, bytes = m.bytes, lastLoaded = Some(System.currentTimeMillis()))

    logger.info(s"using cache eviction strategy: $evictionStrategy")
    logger.info(s"max cache size is ${evictionStrategy.CACHE_SIZE}")

    val wouldRemove = evictionStrategy.wouldRemove(entry, materializations.values.toSeq)

    val wouldRemoveBenefit = wouldRemove.map(_.benefit.toMillis).sum
    logger.debug(s"${ps.eviction} would remove total benefit: ${wouldRemoveBenefit / 1000} sec: \n ${wouldRemove.mkString("\n")}")

    val inserted = if(!ps.admissionCheck || wouldRemoveBenefit < entry.benefit.toMillis) {
      logger.debug(s"removing ${wouldRemove.size} cached entries (${wouldRemoveBenefit / 1000} sec) to add $entry")
      replace(wouldRemove, entry)

      val json = write(materializations)

      Files.write(Conf.materializationMapFile,
        List(json).asJava,
        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)

      val inserted = materializations.contains(entry.lineage)
      if(inserted)
        entry.markWritten

      inserted
    } else {
      logger.debug(s"will not add $entry (admission check: ${ps.admissionCheck}, new entry has higher benefit: ${wouldRemoveBenefit < entry.benefit.toMillis}")
      false
    }


    inserted

  }

  def insert(sig: Lineage, file: URI): Boolean = {
    val entry = CacheEntry(sig, file.toString, _benefit = Long.MinValue + 1, bytes = -1, written = Some(System.currentTimeMillis()), fixed = true)

    materializations += sig -> entry

    val json = write(materializations)

    Files.write(Conf.materializationMapFile,
      List(json).asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)

    true
  }

  private def replace(old: Seq[CacheEntry], entry: CacheEntry): Unit = {
    materializations = materializations.filterNot{ case (_,e) => old.contains(e)} + (entry.lineage -> entry)
  }
}

case class CacheEntry(lineage: Lineage, uri: String, _benefit: Long, bytes: Long, var lastLoaded: Option[Long] = None, var written: Option[Long] = None,
                      var useCount: Int = 0, var fixed: Boolean = false) {


  def benefit: Duration = _benefit.milliseconds

  def markWritten = written = Some(System.currentTimeMillis())

  def markLoaded = {
    lastLoaded = Some(System.currentTimeMillis())
    useCount += 1
  }

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

  override def equals(obj: scala.Any): Boolean = obj match {
    case o: CacheEntry =>
      o.lineage equals lineage
    case _ => false
  }

  override def hashCode(): Int = lineage.hashCode
}