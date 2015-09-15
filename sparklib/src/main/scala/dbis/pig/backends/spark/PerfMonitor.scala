package dbis.pig.backends.spark

import org.apache.spark.scheduler._
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.ListBuffer
import scalikejdbc._

/**
 * Created by kai on 09.09.15.
 */
class PerfMonitor(driver: String = "org.h2.Driver",
    url: String = "jdbc:h2:file:./db/default",
    user: String = "sa",
    pw: String = "")
  extends SparkListener {
  
  private val lineLineageMapping = MutableMap.empty[Int, String]
  
  private val submissionTimes = MutableMap.empty[Int, Long]
  private val b = ListBuffer.empty[(String, Long)]
  
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    
    // get the line number of the stages last operation
    val line = getLineNumber(stageCompleted.stageInfo)

    if(lineLineageMapping.contains(line)) {
      val startTime = submissionTimes(stageCompleted.stageInfo.stageId)
      val duration = stageCompleted.stageInfo.completionTime.get - startTime
      val opLineage = lineLineageMapping(line)
      
      b += ((opLineage, duration))
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val si = stageSubmitted.stageInfo
    
    // XXX: Does the current time match the submission time?
    val time = if(si.submissionTime.isDefined) si.submissionTime.get else System.currentTimeMillis()
    
    submissionTimes += (si.stageId -> time)
  }
  
  def register(line: Int, lineage: String) {
    lineLineageMapping += (line -> lineage)
  }
  
  def flush() {
    
      
    val entrySet = b.map{ case (l,t) => Seq('lineage -> l, 'duration -> t) }.toSeq
    
    DB autoCommit { implicit session => 
      sql"create table if not exists exectimes(lineage varchar(200), duration int)"
        .execute
        .apply()
  }
    
    DB localTx { implicit session =>
      sql"insert into exectimes(lineage, duration) VALUES({lineage},{duration})"
        .batchByName(entrySet:_ *)
        .apply()
    }
    
//    val entries = DB readOnly { implicit session => 
//      sql"select * from exectimes"
//        .map{ rs => s"${rs.string("lineage")}  -->  ${rs.int("duration")}" }
//        .list
//        .apply()
//    }
//    entries.foreach { println }
      
    
  }
  
  private def getLineNumber(stageInfo: StageInfo) = stageInfo.name.split(":")(1).toInt
}
