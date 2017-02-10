package dbis.piglet.backends.spark

/**
 * A performance monitor to collect Spark job statistics. It extends {{SparkListener}}
 * which allows to register it at the SparkContext and get notified, when a Task or Stage
 * is submitted or finished. 
 * 
 * We use the provided information to collect statistics about runtimes and result sizes of a stage 
 */
object PerfMonitor {
  
  def notify(url: String, lineage: String, partitionId: Long, time: Long) = /*Future*/ {
    val dataString = s"${lineage};${partitionId};${time}"
    scalaj.http.Http(url).method("HEAD").param("data", dataString).asString
  }
}

