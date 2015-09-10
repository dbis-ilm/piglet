package dbis.pig.backends.spark

import org.apache.spark.scheduler._
import scala.collection.mutable.Map

/**
 * Created by kai on 09.09.15.
 */
class PerfMonitor extends SparkListener {
  val stages: Map[Int, String] = Map()

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    println("onJobEnd")
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    println("onJobStart")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stages(stageCompleted.stageInfo.stageId) match {
      case Some(lineage) => println("onStageCompleted -> " + lineage
        + " : time = " + stageCompleted.stageInfo.completionTime
        + ", #tasks = " + stageCompleted.stageInfo.numTasks
        + ", id = " + stageCompleted.stageInfo.stageId)
      case None => println("onStageCompleted -> " + stageCompleted.stageInfo.name
        + " : time = " + stageCompleted.stageInfo.completionTime
        + ", #tasks = " + stageCompleted.stageInfo.numTasks
        + ", id = " + stageCompleted.stageInfo.stageId)
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    println("onStageSubmitted")
  }

  def registerStage(stageId: Int, lineage: String): Unit = {
    stages += (stageId -> lineage)
  }
}
