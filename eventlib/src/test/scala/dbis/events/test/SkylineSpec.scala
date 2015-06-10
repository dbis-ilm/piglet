package dbis.events.test

import dbis.events.Skyline
import dbis.events.Skyline.TupleList
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.mutable

/**
 * Created by kai on 09.06.15.
 */
class SkylineSpec extends FlatSpec with Matchers {
  "The bitset mapper" should "produce a correct BitSet for 2D data" in {
    val numDims = 2
    val numPPD = 4
    val inp = List(List(100.0, 300), List(300.0, 800.0), List(600.0, 600.0), List(600.0, 800.0), List(800.0, 0), List(800.0, 900.0))
    val res = Skyline.bitsetMapper(inp.iterator, numDims, numPPD)
    res.hasNext should be (true)
    val bset = res.next()
    bset should be (mutable.BitSet(1, 7, 10, 11, 12, 15))
  }

  it should "produce a correct BitSet also for 3D data" in {

  }

  "The bitset reducer" should "combine BitSets for 2D data" in {
    val res1 = Skyline.bitsetReducer(mutable.BitSet(1, 7, 9), mutable.BitSet(6, 12, 14), 2, 4)
    res1 should be (mutable.BitSet(1, 9, 12))

    val res2 = Skyline.bitsetReducer(mutable.BitSet(3, 6, 9), mutable.BitSet(3, 6, 12), 2, 4)
    res2 should be (mutable.BitSet(3, 6, 9, 12))

    val res3 = Skyline.bitsetReducer(mutable.BitSet(0, 4, 8, 12), mutable.BitSet(0, 4, 8), 2, 4)
    res3 should be (mutable.BitSet(0, 4, 8, 12))

    val res4 = Skyline.bitsetReducer(mutable.BitSet(0, 1, 2, 3), mutable.BitSet(1, 2, 3), 2, 4)
    res4 should be (mutable.BitSet(0, 1, 2, 3))
  }

  "The insertIntoSkyline function" should "construct a skyline from a list of 2D points" in {
    def asNum(a: Any) = Skyline.asNumber(a)
    def dominates(p1: Skyline.SkylineTuple, p2: Skyline.SkylineTuple): Boolean = {
      (asNum(p1(0)) <= asNum(p2(0)) && asNum(p1(1)) < asNum(p2(1))) ||
        (asNum(p1(0)) < asNum(p2(0)) && asNum(p1(1)) <= asNum(p2(1)))
    }

    var skyline: TupleList = List()
    val inp = List(List(10, 50), List(9, 70), List(15, 55), List(15, 65), List(30, 20), List(40, 15), List(35, 70), List(60, 80))
    inp.foreach{ point => skyline = Skyline.insertIntoSkyline(skyline, point, dominates) }
    skyline should contain theSameElementsAs (List(List(10, 50), List(9, 70), List(30, 20), List(40, 15)))

  }

  it should "construct also a skyline from a list of 3D points" in {

  }
}
