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
import scala.io.Source

object CacheManager extends PigletLogging {

  implicit val formats = Serialization.formats(NoTypeHints)

  /**
    * Already existing materializations
    *
    * They're read from file and stored as a mapping from lineage --> file name
    */
  protected[mm] var materializations: Map[Lineage, CacheEntry] = if (Files.exists(Conf.materializationMapFile)) {
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
  def getDataFor(lineage: Lineage): Option[URI] = materializations.get(lineage).map{ e =>
    e.markLoaded()
    new URI(e.uri)
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


    if(entry.bytes > evictionStrategy.CACHE_SIZE) {
      logger.debug(s"${entry.lineage} does not fit into cache (exceeds by ${evictionStrategy.CACHE_SIZE - entry.bytes} bytes)")
      return false
    }

    val wouldRemove = evictionStrategy.wouldRemove(entry, materializations.values.toSeq)

    val wouldRemoveBenefit = wouldRemove.map(_.benefit.toMillis).sum
    logger.debug(s"${ps.eviction} would remove total benefit: ${wouldRemoveBenefit / 1000} sec: \n ${wouldRemove.mkString("\n")}")

    val inserted = if(!ps.admissionCheck || wouldRemoveBenefit < entry.benefit.toMillis) {
      logger.debug(s"removing ${wouldRemove.size} cached entries (${wouldRemoveBenefit / 1000} sec) to add $entry")
      replace(wouldRemove, entry)

      // when data has been replaced, we need to persist these changed immediately
      val json = write(materializations)

      Files.write(Conf.materializationMapFile,
        List(json).asJava,
        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)

      val ins = materializations.contains(entry.lineage)
      if(ins)
        entry.markWritten()

      ins
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

