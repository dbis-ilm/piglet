package dbis.piglet.backends.spark

import java.net.{HttpURLConnection, URL, URLEncoder}

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{NarrowDependency, ShuffleDependency}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map => MutableMap}

class UpdateMap[K,V](m: MutableMap[K,V]) {

  def insertOrUpdate(k: K)( f: Option[V] => V): Unit = {

    if(m.contains(k)) {
      m(k) = f(Some(m(k)))
    } else {
      m(k) = f(None)
    }
  }
}

object PerfMonitor {

  val SEQ_SAMPLE_MAX_SIZE = 8 * 100 * 1024 // 8 kB


  def request(url: String, data: String): Boolean = {
    val theUrl = new URL(s"$url?data=${URLEncoder.encode(data, "UTF-8")}")
    val c = theUrl.openConnection().asInstanceOf[HttpURLConnection]
    c.setRequestMethod("HEAD")
    c.setUseCaches(false)

    c.getResponseCode == HttpURLConnection.HTTP_OK
  }

  // keep in sync with [[dbis.piglet.mm.StatsWriterActor]]
  final val FIELD_DELIM = ";"
  final val PARENT_DELIM = ","
  final val DEP_DELIM = "#"

//  def sizes3(url: String, m: scala.collection.mutable.Map[String, SizeStat]) = {
//    val dataString = m.map{case(lineage, SizeStat(records, bytes)) => s"$lineage:$records:$bytes"}.mkString(FIELD_DELIM)
//    request(url, dataString)
//  }

  def sizes(url: String, m: scala.collection.mutable.Map[String, SizeStat2]) = {
    val dataString = m.map { case (lineage, stat) =>
      s"$lineage:${stat.cnt}:${stat.numBytes()}"
    }.mkString(FIELD_DELIM)

    request(url,dataString)
  }

  def notify(url: String, lineage: String, rdd: RDD[_], partitionId: Int, time: Long) = {

    /* Create the list of parent partitions
     *
     * In theory, each partition can have multiple dependencies, i.e. to the different
     * input RDDs in a join/cogroup. For each dependency, there might be multiple
     * partitions as input. I.e., we have a list of lists
     *
     * The parents string will encode this as:
     *
     * p1-of-dep1,p2-of-dep1,p3-of-dep1#p1-of-dep2,p2-of-dep2#p1-of-dep3#...
     *
     * There are two types of dependencies:
     *  - Narrow dependencies where one partition depends on one (one-to-one) or
     *    many (range) parent partitions.
     *  - Shuffle dependency where new partitions have to be created
     *
     *  For narrow deps it is easy to decide get the parent partitions, whereas for shuffle deps
     *  theoretically all parent partitions server as input for a partition
     */
    val parents = if(rdd != null) {

      val a = rdd.dependencies.map{
        case d: NarrowDependency[_] =>
          d.getParents(partitionId).toList
        case s: ShuffleDependency[_,_,_] =>
          (0 until s.partitioner.numPartitions).toList
//          rdd.partitions.indices
        case d@ _ =>
          println(s"Unknown dependency type: $d")
          List.empty[Int]
      }

      a.map(inner => inner.mkString(PARENT_DELIM)).mkString(DEP_DELIM)

    } else // no RDD given as input -> encode as empty string
      ""

    val dataString = s"$lineage$FIELD_DELIM$partitionId$FIELD_DELIM$parents$FIELD_DELIM$time"
    request(url, dataString)
  }


//  def serializedSize(o: Any): Int = o match {
//    case Boolean => 1
//    case Short | Char => 2
//    case Int | Float  => 4
//    case Long | Double => 8
//    case s: String => s.getBytes.length
//    case t: SchemaClass => t.getNumBytes
//    case l: mutable.Iterable[_] =>
//      l.size * l.headOption.map(serializedSize).getOrElse(-1)
////      l.foldLeft(0)(_ + serializedSize(_))
//    case a: Array[_] =>
//      a.length * a.headOption.map(serializedSize).getOrElse(-1)
//    case _ =>
//      println(s"Unknown type ${o.getClass.getName} use 0 size")
//      -2
//
//  }

//  /**
//    * Estimate the size in bytes occupied by an object
//    *
//    * @param o The object
//    * @return The number of bytes used by the given object
//    */
//  @inline
//  def estimateSize(o: AnyRef): Long = org.apache.spark.util.SizeEstimator.estimate(o)

  @inline
  def sampleSize(t: AnyRef, lineage: String, accum: SizeAccumulator, randFactor: Int, num: Int = 1): Unit = {

    if(accum.containsNot(lineage) || scala.util.Random.nextInt(randFactor) == 0)
      accum.incr(lineage, t, num)
  }

  @inline
  def sampleSize(t: Iterable[AnyRef], lineage: String, accum: SizeAccumulator, randFactor: Int): Unit = {
    sampleSize(t.toSeq, lineage, accum, randFactor)
  }

  @inline
  def sampleSize(t: Seq[AnyRef], lineage: String, accum: SizeAccumulator, randFactor: Int): Unit = {

    if(accum.containsNot(lineage) || scala.util.Random.nextInt(randFactor) == 0) {
      var takenSize = 0L
      val taken = t.takeWhile { s =>
        val tSize = org.apache.spark.util.SizeEstimator.estimate(s)
        if (takenSize < SEQ_SAMPLE_MAX_SIZE) {
          takenSize += tSize
          true
        } else {
          false
        }

      }

      accum.incr(lineage, taken, num = t.size)
    }
  }


//  @inline
//  def sampleSize2(o: AnyRef, lineage: String, accum: SizeAccumulator2): Unit = {
//    val theSize = estimateSize(o)
//    accum.incr(lineage, theSize)
//  }

}

//case class SizeStat(var records: Long, var bytes: Long) extends Cloneable {
//  override def clone(): SizeStat = SizeStat(records, bytes)
//
//  def add(stat: SizeStat) = {
//    records += stat.records
//    bytes += stat.bytes
//  }
//}

case class SizeStat2(private var elems: ListBuffer[AnyRef], private var _cnt: Long, private var _numBytes: Option[Double] = None,
                     private var additionalBytes: Long = 0, private var numAdditional: Int = 0) extends Cloneable {
  override def clone(): SizeStat2 = SizeStat2(elems.clone(), cnt, _numBytes, additionalBytes, numAdditional)

//  def getElems = elems
  def cnt = _cnt
  def numBytes(): Double = _numBytes.getOrElse {
    val nbytes = (SizeStat2.computeNumBytes(elems) + additionalBytes) / (elems.size + numAdditional).toDouble
    _numBytes = Some(nbytes)
    elems.clear()
    nbytes
  }


  def addAdditionalBytes(bytes: Long, num: Int = 1): Unit = {
    additionalBytes += bytes
    numAdditional += num
  }

  def add(o: AnyRef): Unit = {

    if(_numBytes.isEmpty && elems.size < SizeStat2.MaxSampleSizePerOp) {
      elems += o
    }

    if(elems.size >= SizeStat2.MaxSampleSizePerOp) {
      val bytes = (SizeStat2.computeNumBytes(elems) + additionalBytes) / (elems.size + numAdditional).toDouble
      _numBytes = Some(bytes)
      elems.clear()
    }

    _cnt += 1
  }


  def merge(other: SizeStat2): Unit = {

    val iter = other.elems.iterator

    additionalBytes += other.additionalBytes
    numAdditional += other.numAdditional

    while(elems.size < SizeStat2.MaxSampleSizePerOp && iter.hasNext) {
      val elem = iter.next()
      elems += elem
    }

    if(elems.size >= SizeStat2.MaxSampleSizePerOp) {
      val bytes = (SizeStat2.computeNumBytes(elems) + additionalBytes) / (elems.size + numAdditional).toDouble
      _numBytes = Some(bytes)
      elems.clear()
    }

    _cnt += other._cnt
  }
}

object SizeStat2 {
  val MaxSampleSizePerOp: Int = 100

  def computeNumBytes(objects: AnyRef): Long = {

//    val objects = stat.elems

    var bos: java.io.ByteArrayOutputStream = null
    var out: java.io.ObjectOutputStream = null

    val bytes = try {
      bos = new java.io.ByteArrayOutputStream()
      out = new java.io.ObjectOutputStream(bos)
      out.writeObject(objects)
      out.flush()
      bos.toByteArray.length
    } catch {
      case e: Throwable =>
        System.err.println(e.getMessage)
        0
    } finally {
      if(bos != null)
        bos.close()

      if(out != null)
        out.close()
    }

    // too imprecise
    //    org.apache.spark.util.SizeEstimator.estimate(elems) / elems.size.toDouble

//    (bytes + stat.additionalBytes) / (objects.size + stat.numAdditional).toDouble
    bytes
  }

  def fromAdditionalBytes(bytes: Long): SizeStat2 = SizeStat2(ListBuffer.empty, 1, None, bytes, 1)
}

class SizeAccumulator() extends AccumulatorV2[mutable.Map[String, SizeStat2],mutable.Map[String, SizeStat2]] {

  type Lineage = String

  private val theValue = mutable.Map.empty[Lineage, SizeStat2]

  override def isZero: Boolean = theValue.isEmpty

  override def copy(): AccumulatorV2[MutableMap[Lineage, SizeStat2], MutableMap[Lineage, SizeStat2]] = {
    val newAccum = new SizeAccumulator()

    theValue.foreach{ case (k,v) =>
      newAccum.value += k -> v.clone()
    }

    newAccum
  }

  override def reset(): Unit = theValue.clear()

  override def add(others: MutableMap[Lineage, SizeStat2]): Unit = {
    others.foreach{ case (k,v) =>

      if(theValue.contains(k)) {
        theValue(k).merge(v)
      } else {
        theValue += k -> v
      }

    }
  }

  def incr(lineage: Lineage, o: AnyRef, num: Int = 1) = {
    if(theValue.contains(lineage)) {
      theValue(lineage).add(o)
    } else {
      theValue += lineage -> SizeStat2(ListBuffer(o),1)
    }

    if(num > 1)
      theValue(lineage).addAdditionalBytes(SizeStat2.computeNumBytes(o) * (num - 1) )
  }

  def incr(lineage: Lineage, o: Seq[AnyRef], num: Int) = {
    val bytes = if(o.isEmpty) { 0 } else {
      ((SizeStat2.computeNumBytes(o) / o.size.toDouble) * num).toLong
    }
    if(theValue.contains(lineage)) {
      theValue(lineage).addAdditionalBytes(bytes, 1)
    } else {
      theValue += lineage -> SizeStat2.fromAdditionalBytes(bytes)
    }
  }

  def contains(lineage: Lineage): Boolean = theValue.contains(lineage)

  def containsNot(lineage: Lineage): Boolean = ! theValue.contains(lineage)

  override def merge(other: AccumulatorV2[MutableMap[Lineage, SizeStat2], MutableMap[Lineage, SizeStat2]]): Unit = {
    add(other.value)
  }

  override def value: MutableMap[Lineage, SizeStat2] = theValue
}

//class SizeAccumulator2() extends AccumulatorV2[mutable.Map[String, SizeStat],mutable.Map[String, SizeStat]] {
//
//  type Lineage = String
//
//  private val theValue = mutable.Map.empty[Lineage, SizeStat]
//
//  override def isZero: Boolean = theValue.isEmpty
//
//  override def copy(): AccumulatorV2[MutableMap[Lineage, SizeStat], MutableMap[Lineage, SizeStat]] = {
//    val newAccum = new SizeAccumulator2()
//
//
//    theValue.foreach{ case (k,v) =>
//      newAccum.value += k -> v
//    }
//
//    newAccum
//  }
//
//  override def reset(): Unit = theValue.clear()
//
//  override def add(value: MutableMap[Lineage, SizeStat]): Unit = {
//    value.foreach{ case (k,v) =>
//      if(theValue.contains(k)) {
//        theValue(k).add(v)
//      } else {
//        theValue += k -> v
//      }
//    }
//  }
//
//  def incr(lineage: Lineage, bytes: Long) = {
//    if(theValue.contains(lineage)) {
//      theValue(lineage).add(SizeStat(1,bytes))
//    } else {
//      theValue += lineage -> SizeStat(1,bytes)
//    }
//  }
//
//  override def merge(other: AccumulatorV2[MutableMap[Lineage, SizeStat], MutableMap[Lineage, SizeStat]]): Unit = {
//    add(other.value)
//  }
//
//  override def value: MutableMap[Lineage, SizeStat] = theValue
//}