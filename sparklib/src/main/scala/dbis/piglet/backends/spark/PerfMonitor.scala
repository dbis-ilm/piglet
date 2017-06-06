package dbis.piglet.backends.spark

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

/**
 * A performance monitor to collect Spark job statistics. It extends {{SparkListener}}
 * which allows to register it at the SparkContext and get notified, when a Task or Stage
 * is submitted or finished.
 *les
 * We use the provided information to collect statistics about runtimes and result sizes of a stage
 */
class PerfMonitor(url: String) extends SparkListener {

  private val lineLineageMapping = MutableMap.empty[Int, String]
  private val stageSizeMapping = MutableMap.empty[Int, MutableMap[Long, (Long,Long,Long,Long)]].withDefault(MutableMap.empty)


  implicit def createUpdateMap[K,V](m: MutableMap[K,V]): UpdateMap[K,V] = new UpdateMap[K,V](m)

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {

    if(!taskEnd.taskInfo.successful)
      return

    val inputBytes = taskEnd.taskMetrics.inputMetrics.bytesRead
    val inputRecords = taskEnd.taskMetrics.inputMetrics.recordsRead

    val outputBytes = taskEnd.taskMetrics.outputMetrics.bytesWritten
    val outputRecords = taskEnd.taskMetrics.outputMetrics.recordsWritten

    val stageId = taskEnd.stageId
    val taskId = taskEnd.taskInfo.taskId

    stageSizeMapping(stageId).insertOrUpdate(taskId){
      case Some((ib,ir,ob,or)) => (ib + inputBytes, ir + inputRecords, ob + outputBytes, or + outputRecords)
      case None => (inputBytes, inputRecords, outputBytes, outputRecords)
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {

    if(stageCompleted.stageInfo.failureReason.isDefined)
      return

    val stageId = stageCompleted.stageInfo.stageId

    val (inputBytes, inputRecords, outputBytes, outputRecords) = stageSizeMapping(stageId).values.reduceLeft{ (l,r) =>
      (l._1 + r._1, l._2 + r._2, l._3 + r._3, l._4 + r._4)
    }

    val dataString = s"${}${}$inputBytes$FIELD_DELIM$inputRecords$FIELD_DELIM$outputBytes$FIELD_DELIM$outputRecords"
    scalaj.http.Http(url).method("HEAD").param("metrics", dataString).asString
  }

  def register(line: Int, lineage: String) {
    lineLineageMapping += (line -> lineage)
  }
}


object PerfMonitor {

  // keep in sync with [[dbis.piglet.mm.StatsWriterActor]]
  final val FIELD_DELIM = ";"
  final val PARENT_DELIM = ","
  final val DEP_DELIM = "#"

  def sizes(url: String, m: scala.collection.Map[String, Option[Long]]) = {
    val dataSring = m.filter(_._2.isDefined).map{case(k,v) => s"$k=${v.get}"}.mkString(FIELD_DELIM)
    scalaj.http.Http(url).method("HEAD").param("data",dataSring).asString
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
    scalaj.http.Http(url).method("HEAD").param("data", dataString).asString
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