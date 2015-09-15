package dbis.pig.backends.spark

import org.apache.spark.scheduler._
import scala.collection.mutable.{Map => MutableMap}

/**
 * Created by kai on 09.09.15.
 */
class PerfMonitor extends SparkListener {
  
  val lineLineageMapping = MutableMap.empty[Int, String]
  
  val submissionTimes = MutableMap.empty[Int, Long]
  
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    println("onJobEnd")
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    println("onJobStart")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
//    println("onStageCompleted -> " + stageCompleted.stageInfo.name + " : " + stageCompleted.stageInfo.completionTime)
//    println(s"id: ${stageCompleted.stageInfo.stageId}")
//    println(s"complete: ${stageCompleted.stageInfo.stageId}  ${stageCompleted.stageInfo.name}")
    
    // get the line number of the stages last operation
    val line = getLineNumber(stageCompleted.stageInfo)
    if(lineLineageMapping.contains(line)) {
      val startTime = submissionTimes(stageCompleted.stageInfo.stageId)
      val duration = stageCompleted.stageInfo.completionTime.get - startTime
      val opLineage = lineLineageMapping(line)
      println(s"DURATION: $opLineage --> $duration")
    }
    else
      println(s"UNKNOWN OP AT LINE $line")
    
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
//    println(s"submit: ${stageSubmitted.stageInfo.stageId}  ${stageSubmitted.stageInfo.name}")
    
//    submissionTimes += (stageSubmitted.stageInfo.stageId -> stageSubmitted.stageInfo.submissionTime.get)
    submissionTimes += (stageSubmitted.stageInfo.stageId -> System.currentTimeMillis())
  }
  
  def register(line: Int, lineage: String) {
    lineLineageMapping += (line -> lineage)
  }

  
  private def getLineNumber(stageInfo: StageInfo) = stageInfo.name.split(":")(1).toInt
}
