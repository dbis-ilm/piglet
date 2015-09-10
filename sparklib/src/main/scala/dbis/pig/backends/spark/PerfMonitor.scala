package dbis.pig.backends.spark

import org.apache.spark.scheduler._

/**
 * Created by kai on 09.09.15.
 */
class PerfMonitor extends SparkListener {
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    println("onJobEnd")
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    println("onJobStart")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    println("onStageCompleted -> " + stageCompleted.stageInfo.name + " : " + stageCompleted.stageInfo.completionTime)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    println("onStageSubmitted")
  }

}
