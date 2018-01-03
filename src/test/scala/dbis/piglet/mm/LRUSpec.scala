package dbis.piglet.mm

import dbis.piglet.tools.CliParams
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}
import scala.concurrent.duration._

class LRUSpec extends FlatSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    val ps = new ProfilerSettings(minBenefit = 10.seconds,
      admissionCheck = false,
      cacheSize = 20,
      eviction = EvictionStrategy.LRU)

    CliParams._values = new CliParams(profiling = Some(ps))
  }

  lazy val strategy = EvictionStrategy.getStrategy(CliParams.values.profiling.get.eviction)


  "LRU" should "remove one element that does not fit from 2-elem cache" in {

    val newEntry = CacheEntry("new", "new", 1000L, 10, lastLoaded = Some(11))

    val list = Seq(
      CacheEntry("1", "1", _benefit=1000L, bytes=10, lastLoaded = Some(10)),
      CacheEntry("2", "2", _benefit=1000L, bytes=10, lastLoaded = Some(9))
    )

    val wouldRemove = strategy.wouldRemove(newEntry, list)

    wouldRemove should contain only CacheEntry("2", "2", _benefit=1000L, bytes=10, lastLoaded = Some(9))
  }

  it should "remove two entries from a 5-elem cache" in {

    val newEntry = CacheEntry("new", "new", 1000L, 7, lastLoaded = Some(12))

    val list = Seq(
      CacheEntry("1", "1", _benefit=1000L, bytes=4, lastLoaded = Some(10)),
      CacheEntry("2", "2", _benefit=1000L, bytes=5, lastLoaded = Some(9)),
      CacheEntry("3", "3", _benefit=1000L, bytes=1, lastLoaded = Some(11)),
      CacheEntry("4", "4", _benefit=1000L, bytes=7, lastLoaded = Some(11)),
      CacheEntry("5", "5", _benefit=1000L, bytes=3, lastLoaded = Some(8))
    )

    val wouldRemove = strategy.wouldRemove(newEntry, list)

    wouldRemove should contain only (
      CacheEntry("2", "2", _benefit=1000L, bytes=5, lastLoaded = Some(9)),
      CacheEntry("5", "5", _benefit=1000L, bytes=3, lastLoaded = Some(8)))

  }

  it should "not add a new entry if it does not fit into cache" in {
    val newEntry = CacheEntry("new", "new", 1000L, 27, lastLoaded = Some(12))

    val list = Seq(
      CacheEntry("1", "1", _benefit=1000L, bytes=4, lastLoaded = Some(10)),
      CacheEntry("2", "2", _benefit=1000L, bytes=5, lastLoaded = Some(9)),
      CacheEntry("3", "3", _benefit=1000L, bytes=1, lastLoaded = Some(11)),
      CacheEntry("4", "4", _benefit=1000L, bytes=7, lastLoaded = Some(11)),
      CacheEntry("5", "5", _benefit=1000L, bytes=3, lastLoaded = Some(8))
    )

    val wouldRemove = strategy.wouldRemove(newEntry, list)

    wouldRemove should contain only newEntry
  }

  it should "remove all others if new entry fills cache alone" in {
    val newEntry = CacheEntry("new", "new", 1000L, 20, lastLoaded = Some(12))

    val list = Seq(
      CacheEntry("1", "1", _benefit=1000L, bytes=4, lastLoaded = Some(10)),
      CacheEntry("2", "2", _benefit=1000L, bytes=5, lastLoaded = Some(9)),
      CacheEntry("3", "3", _benefit=1000L, bytes=1, lastLoaded = Some(11)),
      CacheEntry("4", "4", _benefit=1000L, bytes=7, lastLoaded = Some(11)),
      CacheEntry("5", "5", _benefit=1000L, bytes=3, lastLoaded = Some(8))
    )

    val wouldRemove = strategy.wouldRemove(newEntry, list)

    wouldRemove should contain only (list:_*)
  }

  it should "simply add a new value that fits into cache" in {
    val newEntry = CacheEntry("new", "new", 1000L, 1, lastLoaded = Some(12))

    val list = Seq(
      CacheEntry("1", "1", _benefit=1000L, bytes=4, lastLoaded = Some(10)),
      CacheEntry("2", "2", _benefit=1000L, bytes=5, lastLoaded = Some(9)),
      CacheEntry("3", "3", _benefit=1000L, bytes=1, lastLoaded = Some(11)),
      CacheEntry("4", "4", _benefit=1000L, bytes=7, lastLoaded = Some(11)),
      CacheEntry("5", "5", _benefit=1000L, bytes=2, lastLoaded = Some(8))
    )

    val wouldRemove = strategy.wouldRemove(newEntry, list)

    wouldRemove shouldBe 'empty
  }

}
