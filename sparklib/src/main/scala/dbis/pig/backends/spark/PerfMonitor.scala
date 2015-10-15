package dbis.pig.backends.spark

import org.apache.spark.scheduler._
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.ListBuffer
import scalikejdbc._

/**
 * Created by kai on 09.09.15.
 */
class PerfMonitor(appName: String, driver: String = "org.h2.Driver",
    url: String = "jdbc:h2:tcp://cloud01.prakinf.tu-ilmenau.de/data/stha1in/pigletdb",
    user: String = "sa",
    pw: String = "")
  extends SparkListener {
  
  private val progStartTime = System.currentTimeMillis()
  
  private val lineLineageMapping = MutableMap.empty[Int, String]
  
//  private val submissionTimes = MutableMap.empty[Int, Long]
//  private val b = ListBuffer.empty[(String, Long, Long)]
  
  
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = this.synchronized {
    
     DB localTx { implicit session =>
      sql"""insert into exectimes(appname, stageid, stagename, lineage, stageduration, progduration, submissiontime, completiontime) VALUES(
              ${appName},
              ${stageCompleted.stageInfo.stageId},
              ${stageCompleted.stageInfo.name},
              ${lineLineageMapping.getOrElse(stageCompleted.stageInfo.name.split(":")(1).toInt, null) },
              ${stageCompleted.stageInfo.completionTime.get - stageCompleted.stageInfo.submissionTime.get},
              ${stageCompleted.stageInfo.completionTime.get - progStartTime},
              ${stageCompleted.stageInfo.submissionTime.get},
              ${stageCompleted.stageInfo.completionTime.get}
            )"""
        .update
        .apply()
      }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
//    val si = stageSubmitted.stageInfo
//    
//    // XXX: Does the current time match the submission time?
//    val time = if(si.submissionTime.isDefined) si.submissionTime.get else System.currentTimeMillis()
//    
//    submissionTimes += (si.stageId -> time)
  }
  
  def register(line: Int, lineage: String) {
    lineLineageMapping += (line -> lineage)
  }
  
  def flush() {
////        Class.forName(driver)
////    ConnectionPool.singleton(url, user, pw)
////      try {
//    val entrySet = b.map{ case (l,s,p) => Seq('lineage -> l, 'stageduration -> s, 'progduration -> p) }.toSeq
//    
//    DB autoCommit { implicit session => 
//      sql"create table if not exists exectimes(lineage varchar(200) primary key, stageduration bigint, progduration bigint, ts timestamp default current_timestamp())"
//        .execute
//        .apply()
//    }
//    
//    DB localTx { implicit session =>
//      sql"insert into exectimes(lineage, stageduration, progduration) VALUES({lineage},{stageduration},{progduration})"
//        .batchByName(entrySet:_ *)
//        .apply()
//    }
////    } finally {
////      ConnectionPool.closeAll()
////    }
  }
}
