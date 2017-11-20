package dbis.piglet.backends.spark

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

  def sizes(url: String, m: scala.collection.mutable.Map[String, SizeStat]) = {
    val dataString = m.map{case(lineage, SizeStat(records, bytes)) => s"$lineage:$records:$bytes"}.mkString(FIELD_DELIM)
    request(url, dataString)
  }

  def sizes2(url: String, m: scala.collection.mutable.Map[String, ListBuffer[SchemaClass]]) = {
    val dataString = m.map { case (lineage, objects) =>
      val numBytes = {
        var bos: java.io.ByteArrayOutputStream = null
        var out: java.io.ObjectOutputStream = null

        try {
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
      }

      s"$lineage:${objects.size}:$numBytes"

    }.mkString(FIELD_DELIM)

    request(url,dataString)
  }

  def sizes3(url: String, m: scala.collection.mutable.Map[String, ListBuffer[SchemaClass]]) = {
    val dataString = m.map { case (lineage, objects) =>
      val numBytes = org.apache.spark.util.SizeEstimator.estimate(objects)

      s"$lineage:${objects.size}:$numBytes"

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

  /**
    * Estimate the size in bytes occupied by an object
    *
    * @param o The object
    * @return The number of bytes used by the given object
    */
  @inline
  def estimateSize(o: AnyRef): Long = org.apache.spark.util.SizeEstimator.estimate(o)

  @inline
  def sampleSize2(t: SchemaClass, lineage: String, accum: SizeAccumulator3): Unit = {
    accum.incr(lineage, t)
  }

  @inline
  def sampleSize(o: AnyRef, lineage: String, accum: SizeAccumulator2): Unit = {
    val theSize = estimateSize(o)
    accum.incr(lineage, theSize)
  }

}

case class SizeStat(var records: Long, var bytes: Long) extends Cloneable {
  override def clone(): SizeStat = SizeStat(records, bytes)

  def add(stat: SizeStat) = {
    records += stat.records
    bytes += stat.bytes
  }
}

case class SizeStat2(var elems: ListBuffer[SchemaClass]) extends Cloneable {
  override def clone(): SizeStat2 = SizeStat2(elems.clone())

  def add(stat: SizeStat2): Unit = {
    elems ++= stat.elems
  }
}

class SizeAccumulator3(maxSampleSizePerOp: Int = 1000 ) extends AccumulatorV2[mutable.Map[String, ListBuffer[SchemaClass]],mutable.Map[String, ListBuffer[SchemaClass]]] {

  type Lineage = String

  private val theValue = mutable.Map.empty[Lineage, ListBuffer[SchemaClass]]

  override def isZero: Boolean = theValue.isEmpty

  override def copy(): AccumulatorV2[MutableMap[Lineage, ListBuffer[SchemaClass]], MutableMap[Lineage, ListBuffer[SchemaClass]]] = {
    val newAccum = new SizeAccumulator3()

    theValue.foreach{ case (k,v) =>
      newAccum.value += k -> v
    }

    newAccum
  }

  override def reset(): Unit = theValue.clear()

  override def add(value: MutableMap[Lineage, ListBuffer[SchemaClass]]): Unit = {
    value.foreach{ case (k,v) =>
      if(theValue.contains(k) && theValue(k).size < maxSampleSizePerOp) {
        val numTake = math.max(maxSampleSizePerOp - theValue(k).size, 0)
        theValue(k) ++= v.take(numTake)
      } else {
        theValue += k -> v
      }
    }
  }

  def incr(lineage: Lineage, o: SchemaClass) = {
    if(theValue.contains(lineage) && theValue(lineage).size < maxSampleSizePerOp) {
      theValue(lineage) += o
    } else {
      theValue += lineage -> ListBuffer(o)
    }
  }

  override def merge(other: AccumulatorV2[MutableMap[Lineage, ListBuffer[SchemaClass]], MutableMap[Lineage, ListBuffer[SchemaClass]]]): Unit = {
    add(other.value)
  }

  override def value: MutableMap[Lineage, ListBuffer[SchemaClass]] = theValue
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