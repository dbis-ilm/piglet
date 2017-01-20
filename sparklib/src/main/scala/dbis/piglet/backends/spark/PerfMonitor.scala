package dbis.piglet.backends.spark

import java.net.URI
import scala.concurrent._
import ExecutionContext.Implicits.global

/**
 * A performance monitor to collect Spark job statistics. It extends {{SparkListener}}
 * which allows to register it at the SparkContext and get notified, when a Task or Stage
 * is submitted or finished. 
 * 
 * We use the provided information to collect statistics about runtimes and result sizes of a stage 
 */
object PerfMonitor {
  
  def notify(url: String, lineage: String, partitionId: Long, time: Long) = /*Future*/ {
    //val jsonString = s"""{"partitionId":"$partitionId","time":"${time}","lineage":"$lineage"}""" 
    //val result = scalaj.http.Http(url).postData(jsonString).header("Content-Type", "application/json").header("Charset","UTF-8").asString
    //println(result)
    val dataString = s"${lineage};${partitionId};${time}"
    scalaj.http.Http(url).method("HEAD").param("data", dataString).asString
  }
  
  
}
