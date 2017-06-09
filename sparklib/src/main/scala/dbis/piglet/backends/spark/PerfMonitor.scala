package dbis.piglet.backends.spark

import java.net.{HttpURLConnection, URL, URLEncoder}

import dbis.piglet.backends.spark.PerfMonitor.FIELD_DELIM
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{NarrowDependency, ShuffleDependency}

import scala.collection.mutable.{Map => MutableMap}

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

  def sizes(url: String, m: scala.collection.Map[String, Option[Long]]) = {
    val dataString = m.filter(_._2.isDefined).map{case(k,v) => s"$k:${v.get}"}.mkString(FIELD_DELIM)
//    scalaj.http.Http(url).method("HEAD").param("data",dataSring).asString
    request(url, dataString)
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
          d.getParents(partitionId)
        case _: ShuffleDependency[_,_,_] =>
          rdd.partitions.indices
        case d@ _ =>
          println(s"Unknown dependency type: $d")
          Seq.empty[Int]
      }
      a.map(inner => inner.mkString(PARENT_DELIM)).mkString(DEP_DELIM)
    } else // no RDD given as input -> encode as empty string
      ""

    val dataString = s"$lineage$FIELD_DELIM$partitionId$FIELD_DELIM$parents$FIELD_DELIM$time"
//    scalaj.http.Http(url).method("HEAD").param("data", dataString).asString
    request(url, dataString)
  }
}

class SizeAccumulator(private var theValue: Option[Long] = None) extends AccumulatorV2[Option[Long],Option[Long]] {

  override def isZero: Boolean = theValue.isEmpty

  override def copy(): AccumulatorV2[Option[Long], Option[Long]] = if(theValue.isEmpty)
    new SizeAccumulator()
  else new SizeAccumulator(Some(theValue.get))


  override def reset(): Unit = theValue = None

  def incr(n: Long = 1L): Unit = add(Some(n))

  def add(n: Long = 1L): Unit = add(Some(n))

  override def add(v: Option[Long]): Unit = if(theValue.isEmpty)
    theValue = v
  else if(v.isDefined)
    theValue = Some(theValue.get + v.get)


  override def merge(other: AccumulatorV2[Option[Long], Option[Long]]): Unit = {
    add(other.value)
  }

  override def value: Option[Long] = theValue
}