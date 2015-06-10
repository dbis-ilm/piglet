package dbis.events

import org.apache.spark.rdd.RDD
import scala.collection.mutable._
import scala.collection.mutable.ListBuffer

/**
 * Created by kai on 02.06.15.
 */

/**
 * Assumptions (input data has to be mapped accordingly):
 * - arbitary number of dimensions
 * - dimensions with domain [0, 1000.0]
 * - skyline on minimum in each dimension
 */
object Skyline {
  type SkylineTuple = List[Any]
  type TupleList = List[SkylineTuple]

  def process(input: RDD[List[Any]], numDims: Int, numPPD: Int, dominatesPredicate: String): RDD[List[Any]] = input

  def asNumber(x: Any) = x match {
    case s: String => 0.0
    case i: java.lang.Number => i.doubleValue()
  }

  def bitsetMapper(iter: Iterator[SkylineTuple], numDims: Int, numPPD: Int) : Iterator[BitSet] = {
    var bits = BitSet.empty
    val res: List[BitSet] = List(bits)
    val partitionSize = 1000.0 / numPPD
    while (iter.hasNext) {
      val obj = iter.next
      val cells = obj.map(v => (asNumber(v) / partitionSize).toInt)
      var pos: Int = 0
      var d = 0
      for (d <- 0 to numDims - 1)
        pos = pos + cells(numDims - 1 - d) * Math.pow(numPPD, d).toInt
      bits += pos
    }
    res.iterator
  }

  def pruneRegions(i: Int, bs: BitSet, numDims: Int, numPPD: Int) : Unit = {
    // TODO: anpassen für mehrdimensionalen Fall
    val x = i / numPPD
    val y = i % numPPD
    for (xi <- x + 1 to numPPD - 1)
      for (yi <- y + 1 to numPPD - 1)
        bs -= xi * numPPD + yi
  }

  def bitsetReducer(bs1: BitSet, bs2: BitSet, numDims: Int, numPPD: Int) : BitSet = {
    var bits = bs1.union(bs2)
    for (i <- bits) {
      pruneRegions(i, bits, numDims, numPPD)
    }
    bits
  }

  def insertIntoSkyline(skyline: TupleList, point: SkylineTuple, dominates: (SkylineTuple, SkylineTuple) => Boolean) : TupleList = {
    /*
     * case #1: if any of the objects in skyline dominates the object point, we just drop it
     */
    if (skyline.exists (dominates(_, point)))
      skyline
    else {
      /*
       * case #2: remove all objects from the skyline which are dominated by object point,
       * and add the new object to the skyline
       */
      point :: skyline.filter(! dominates(point, _))
    }
  }
}
