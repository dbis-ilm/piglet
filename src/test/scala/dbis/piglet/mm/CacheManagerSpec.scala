package dbis.piglet.mm

import java.net.URI
import java.nio.file.{Files, StandardOpenOption}

import dbis.piglet.tools.{CliParams, Conf}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration._




class CacheManagerSpec extends FlatSpec with Matchers with BeforeAndAfter {

  before {
//    import scala.collection.JavaConverters._
//    Files.write(Conf.materializationMapFile, List("").asJava,
//      StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING )


    Files.deleteIfExists(Conf.materializationMapFile)

    CacheManager.materializations = Map(
      "1" -> CacheEntry("1",_benefit = 10, bytes = 100, uri = "1", lastLoaded = Some(1L)),
      "2" -> CacheEntry("2",_benefit = 20, bytes = 200, uri = "2", lastLoaded = Some(2L)),
      "3" -> CacheEntry("3",_benefit = 30, bytes = 300, uri = "3", lastLoaded = Some(3L)),
      "4" -> CacheEntry("4",_benefit = 40, bytes = 400, uri = "4", lastLoaded = Some(4L)),
      "5" -> CacheEntry("5",_benefit = 50, bytes = 500, uri = "5", lastLoaded = Some(5L)),
      "6" -> CacheEntry("6",_benefit = 60, bytes = 600, uri = "6", lastLoaded = Some(6L)),
      "7" -> CacheEntry("7",_benefit = 70, bytes = 700, uri = "7", lastLoaded = Some(7L)),
      "8" -> CacheEntry("8",_benefit = 80, bytes = 800, uri = "8", lastLoaded = Some(8L)),
      "9" -> CacheEntry("9",_benefit = 90, bytes = 900, uri = "9", lastLoaded = Some(9L)),
      "10" -> CacheEntry("10",_benefit = 100, bytes = 1000, uri = "10", lastLoaded = Some(10L))
    )
  }

  "LRU strategy" should "remove one entry with admission check" in {

    val ps = new ProfilerSettings(minBenefit = 10.seconds,
      admissionCheck = true,
      cacheSize = 5500,
      eviction = EvictionStrategy.LRU)

    CliParams._values = new CliParams(profiling = Some(ps))

    val mp = MaterializationPoint("new", prob = 1, cost = 1, bytes = 1100, benefit = 1000.milliseconds)

    val u = new URI("file:///new")

    val inserted = CacheManager.insert(mp, u)

    //    CacheManager.materializations.values.foreach(println)

    inserted shouldBe true

    CacheManager.materializations.size shouldBe 6

    CacheManager.materializations.values should not contain CacheEntry("1",_benefit = 10, bytes = 100, uri = "1", lastLoaded = Some(1L))
    CacheManager.materializations.values should not contain CacheEntry("2",_benefit = 10, bytes = 100, uri = "1", lastLoaded = Some(1L))
    CacheManager.materializations.values should not contain CacheEntry("3",_benefit = 10, bytes = 100, uri = "1", lastLoaded = Some(1L))
    CacheManager.materializations.values should not contain CacheEntry("4",_benefit = 10, bytes = 100, uri = "1", lastLoaded = Some(1L))
    CacheManager.materializations.values should not contain CacheEntry("5",_benefit = 10, bytes = 100, uri = "1", lastLoaded = Some(1L))

    CacheManager.materializations.values should contain (CacheEntry("new", u.toString, 110, 1100))

  }

  it should "remove one entry with NO benefit and NO admission check" in {
    val ps = new ProfilerSettings(minBenefit = 10.seconds,
      admissionCheck = false,
      cacheSize = 5500,
      eviction = EvictionStrategy.LRU)

    CliParams._values = new CliParams(profiling = Some(ps))

    val mp = MaterializationPoint("new",cost = 1, prob = 1, bytes = 100, benefit = 1.milliseconds)

    val u = new URI("file:///new")

    val inserted = CacheManager.insert(mp, u)

    inserted shouldBe true

    CacheManager.materializations.size shouldBe 10

    CacheManager.materializations.values should not contain CacheEntry("1",_benefit = 10, bytes = 100, uri = "1", lastLoaded = Some(1L))
    CacheManager.materializations.values should contain (CacheEntry("new", u.toString, 110, 1100))
  }

  it should "not add an entry because of admission check" in {
    val ps = new ProfilerSettings(minBenefit = 10.seconds,
      admissionCheck = true,
      cacheSize = 5500,
      eviction = EvictionStrategy.LRU)

    CliParams._values = new CliParams(profiling = Some(ps))

    val mp = MaterializationPoint("new",cost = 1, prob = 1, bytes = 100, benefit = 1.milliseconds)

    val u = new URI("file:///new")

    val inserted = CacheManager.insert(mp, u)

    inserted shouldBe false

    CacheManager.materializations.size shouldBe 10

    CacheManager.materializations.values should contain (CacheEntry("1",_benefit = 10, bytes = 100, uri = "1", lastLoaded = Some(1L)))
    CacheManager.materializations.values should not contain CacheEntry("new", u.toString, 110, 1100)
  }

  it should "remove one entry with LRU and NO admission check" in {
    val ps = new ProfilerSettings(minBenefit = 10.seconds,
      admissionCheck = false,
      cacheSize = 5500,
      eviction = EvictionStrategy.LRU)

    CliParams._values = new CliParams(profiling = Some(ps))

    val mp = MaterializationPoint("new",1,1,1100, 1.milliseconds)

    val u = new URI("file:///new")

    val inserted = CacheManager.insert(mp, u)

    inserted shouldBe true

    CacheManager.materializations.values should contain (CacheEntry("new", u.toString, 110, 1100))

  }
}