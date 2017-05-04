package dbis.piglet.backends.spark

import org.apache.spark.{NarrowDependency, ShuffleDependency}
import org.apache.spark.rdd.RDD

/**
 * A performance monitor to collect Spark job statistics. It extends {{SparkListener}}
 * which allows to register it at the SparkContext and get notified, when a Task or Stage
 * is submitted or finished. 
 * 
 * We use the provided information to collect statistics about runtimes and result sizes of a stage 
 */
object PerfMonitor {

  // keep in sync with [[dbis.piglet.mm.StatsWriterActor]]
  final val FIELD_DELIM = ";"
  final val PARENT_DELIM = ","
  final val DEP_DELIM = "#"
  
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
