package dbis.piglet.backends.spark

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.net.{HttpURLConnection, URL, URLEncoder}

import com.sun.xml.internal.ws.wsdl.writer.document.xsd.Schema
import dbis.piglet.backends.SchemaClass
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{NarrowDependency, ShuffleDependency}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map => MutableMap}
import scala.util.Try

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
      }.toList

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
  def sampleSize(t: SchemaClass, lineage: String, accum: SizeAccumulator): Unit = {
    accum.incr(lineage, t)
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

case class SizeStat2(private var _elems: ListBuffer[SchemaClass], private var _cnt: Long, private var _numBytes: Option[Double] = None) extends Cloneable {
  override def clone(): SizeStat2 = SizeStat2(_elems.clone(), _cnt, _numBytes)

  def getElems = _elems
  def cnt = _cnt
  def numBytes(): Double = _numBytes.getOrElse {
    val nbytes = computeNumBytes
    _numBytes = Some(nbytes)
    _elems.clear()
    nbytes
  }

  private def computeNumBytes: Double = {

    var bos: java.io.ByteArrayOutputStream = null
    var out: java.io.ObjectOutputStream = null

    val bytes = try {
      bos = new java.io.ByteArrayOutputStream()
      out = new java.io.ObjectOutputStream(bos)
      out.writeObject(_elems)
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
//    org.apache.spark.util.SizeEstimator.estimate(_elems) / _elems.size.toDouble

    bytes / _elems.size.toDouble
  }


  def add(o: SchemaClass): Unit = {

    if(_numBytes.isEmpty && _elems.size < SizeStat2.MaxSampleSizePerOp) {
      _elems += o
    }

    if(_elems.size >= SizeStat2.MaxSampleSizePerOp) {
      val bytes = computeNumBytes
      _numBytes = Some(bytes)
      _elems.clear()
    }

    _cnt += 1
  }


  def merge(other: SizeStat2): Unit = {

    val iter = other.getElems.iterator

    while(_elems.size < SizeStat2.MaxSampleSizePerOp && iter.hasNext) {
      val elem = iter.next()
      _elems += elem
    }

    if(_elems.size >= SizeStat2.MaxSampleSizePerOp) {
      val bytes = computeNumBytes
      _numBytes = Some(bytes)
      _elems.clear()
    }

    _cnt += other._cnt
  }
}

object SizeStat2 {
  val MaxSampleSizePerOp: Int = 1000
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

  def incr(lineage: Lineage, o: SchemaClass) = {
    if(theValue.contains(lineage)) {
      theValue(lineage).add(o)
    } else {
      theValue += lineage -> SizeStat2(ListBuffer(o),1)
    }
  }

  override def merge(other: AccumulatorV2[MutableMap[Lineage, SizeStat2], MutableMap[Lineage, SizeStat2]]): Unit = {
    add(other.value)
  }

  override def value: MutableMap[Lineage, SizeStat2] = theValue
}

class SizeAccumulator2() extends AccumulatorV2[mutable.Map[String, SizeStat],mutable.Map[String, SizeStat]] {

  type Lineage = String

  private val theValue = mutable.Map.empty[Lineage, SizeStat]

  override def isZero: Boolean = theValue.isEmpty

  override def copy(): AccumulatorV2[MutableMap[Lineage, SizeStat], MutableMap[Lineage, SizeStat]] = {
    val newAccum = new SizeAccumulator2()


    theValue.foreach{ case (k,v) =>
      newAccum.value += k -> v
    }

    newAccum
  }

  override def reset(): Unit = theValue.clear()

  override def add(value: MutableMap[Lineage, SizeStat]): Unit = {
    value.foreach{ case (k,v) =>
      if(theValue.contains(k)) {
        theValue(k).add(v)
      } else {
        theValue += k -> v
      }
    }
  }

  def incr(lineage: Lineage, bytes: Long) = {
    if(theValue.contains(lineage)) {
      theValue(lineage).add(SizeStat(1,bytes))
    } else {
      theValue += lineage -> SizeStat(1,bytes)
    }
  }

  override def merge(other: AccumulatorV2[MutableMap[Lineage, SizeStat], MutableMap[Lineage, SizeStat]]): Unit = {
    add(other.value)
  }

  override def value: MutableMap[Lineage, SizeStat] = theValue
}