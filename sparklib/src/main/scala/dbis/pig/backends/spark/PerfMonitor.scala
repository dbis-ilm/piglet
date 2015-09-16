package dbis.pig.backends.spark

import org.apache.spark.scheduler._
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.ListBuffer
import scalikejdbc._

/**
 * Created by kai on 09.09.15.
 */
class PerfMonitor(driver: String = "org.h2.Driver",
    url: String = "jdbc:h2:tcp://cloud01.prakinf.tu-ilmenau.de/data/stha1in/pigletdb",
    user: String = "sa",
    pw: String = "")
  extends SparkListener {
  
  private val progStartTime = System.currentTimeMillis()
  
  private val lineLineageMapping = MutableMap.empty[Int, String]
  
  private val submissionTimes = MutableMap.empty[Int, Long]
  private val b = ListBuffer.empty[(String, Long, Long)]
  
  
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    
    // get the line number of the stages last operation
    val line = getLineNumber(stageCompleted.stageInfo)

    if(lineLineageMapping.contains(line)) {
      val startTime = submissionTimes(stageCompleted.stageInfo.stageId)
      val stageDuration = stageCompleted.stageInfo.completionTime.get - startTime
      val progDuration = stageCompleted.stageInfo.completionTime.get - progStartTime
      val opLineage = lineLineageMapping(line)
      
      b += ((opLineage, stageDuration, progDuration))
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
//        Class.forName(driver)
//    ConnectionPool.singleton(url, user, pw)
//      try {
    val entrySet = b.map{ case (l,s,p) => Seq('lineage -> l, 'stageduration -> s, 'progduration -> p) }.toSeq
    
    DB autoCommit { implicit session => 
      sql"create table if not exists exectimes(lineage varchar(200), stageduration bigint, progduration bigint)"
        .execute
        .apply()
    }
    
    DB localTx { implicit session =>
      sql"insert into exectimes(lineage, stageduration, progduration) VALUES({lineage},{stageduration},{progduration})"
        .batchByName(entrySet:_ *)
        .apply()
    }
//    } finally {
//      ConnectionPool.closeAll()
//    }
  }
  
  private def getLineNumber(stageInfo: StageInfo) = stageInfo.name.split(":")(1).toInt
}
