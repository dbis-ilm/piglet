package dbis.piglet.mm

import scala.collection.mutable.{Map => MutableMap,ListBuffer}
import scala.collection.JavaConverters._
import scala.io.Source

import java.net.URI
import java.nio.file.Path
import java.nio.file.Files
import java.nio.file.StandardOpenOption

//import org.json4s._
//import org.json4s.native.JsonMethods._

import dbis.piglet.op.PigOperator
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.plan.MaterializationManager
import dbis.piglet.tools.Conf
import dbis.piglet.tools.BreadthFirstTopDownWalker
import dbis.piglet.tools.DepthFirstTopDownWalker
import dbis.piglet.tools.logging.PigletLogging
import dbis.setm.SETM.timing

/**
 * Created by kai on 24.08.15.
 */
object DataflowProfiler extends PigletLogging {
  
  logger.debug(s"using profiling ")
  
  private val cache = MutableMap.empty[String, MaterializationPoint]

//  implicit lazy val formats = org.json4s.DefaultFormats
  
  
//  logger.info(s"Using storage service at $url for execution times")
  
  def getMaterializationPoint(hash: String): Option[MaterializationPoint] = cache.get(hash)

  def addMaterializationPoint(matPoint: MaterializationPoint): Unit = {
    val entry = cache.getOrElse(matPoint.hash, matPoint)
    if (entry.parentHash.nonEmpty && cache.contains(entry.parentHash.get)) {
      // TODO: calculate _cumulative_ benefit
      val parent = cache.get(entry.parentHash.get).get
      val benefit = parent.loadTime + entry.procTime - entry.loadTime
      entry.benefit = parent.benefit + benefit
    }
    entry.count += 1
    cache += (matPoint.hash -> entry)
  }

  override def toString = cache.mkString(",")
  
  
  
  def getExectimes(lineage: String): Option[(Long, Long)] = {
//    if(url.isEmpty) {
//      logger.warn("cannot retreive execution statistics: No URL to storage service set")
//      return None
//    }
//    val u = url.resolve(s"/${Conf.EXECTIMES_FRAGMENT}/$lineage")
//    val result = scalaj.http.Http(u.toString()).asString
//      
//    if(result.isError) {
////      logger.warn(s"Could not retreive exectimes for $lineage: ${result.statusLine}")
//      return None
//    }
//    
//    
//    val json = parse(result.body)
//    
//    
//    if((json \ "exectimes").children.size <= 0)
//      return None
//    
//    val progDurations = (json \ "exectimes" \ "progduration").extract[Seq[Long]]
//    val avgProgDuration = progDurations.sum / progDurations.size 
//    
//    val stageDurations = (json \ "exectimes" \ "stageduration").extract[Seq[Long]]
//    val avgStageDuration = stageDurations.sum / stageDurations.size
//    
//    return Some(avgProgDuration, avgStageDuration)
    None
  }

  /**
   * Go over the plan and and check for existing runtime information for each op (lineage).
   * If something exists, create a materialization point that can be used later. 
   */
  def addMaterializationPoints(plan: DataflowPlan) = timing("identify mat points") {
    
    DepthFirstTopDownWalker.walk(plan) { op =>

      logger.debug( s"""checking storage service for runtime information for operator "${op.lineageSignature}" """)
      // check for the current operator, if we have some runtime/stage information 
      val avg: Option[(Long, Long)] = getExectimes(op.lineageSignature) 

      // if we have information, create a (potential) materialization point
      if (avg.isDefined) {
        val (progduration, stageduration) = avg.get

        logger.debug( s"""found runtime information: program: $progduration  stage: $stageduration""")

        /* XXX: calculate benefit
         * Here, we do not have the parent hash information.
         * But is it still needed? Since we have the program runtime duration 
         * from beginning until the end the stage, we don't need to calculate
         * the cumulative benefit?
         */
        val mp = MaterializationPoint(op.lineageSignature,
          None, // currently, we do not consider the parent
          progduration, // set the processing time
          0L, // TODO: calculate loading time at this point
          0L, // TODO: calculate saving time at this point
          0 // no count/size information so far
        )

        this.addMaterializationPoint(mp)

      } else {
        logger.debug(s" found no runtime information")
      }
    }
    
  }
  
  /**
   * For profiling the jobs that are run with Piglet, we count how often an
   * operator lineage is used within a script. This counter value is added
   * and stored in our database. 
   * <br><br>
   * We traverse the plan and for operator we find, we store its count value
   * in a map. Afterwards, this map's content is stored in our database
   * where the values are added to already existing ones.
   * 
   * @param schedule The schedule, all current plans, to count operators in
   */
  def createOpCounter(schedule: Seq[(DataflowPlan, Path)], file: Path) = timing("analyze plans") {

    logger.debug("start creating lineage counter map")

    val delim = ";"
    
    val map = MutableMap.empty[String, Int].withDefaultValue(0)
    
    if(Files.exists(file)) {
      Source.fromFile(file.toFile()).getLines().foreach{line =>
        val arr = line.split(delim)
        val key = arr(0)
        val value = arr(1).toInt
        
        map += (key -> value)
      }
    }
    
    // the visitor to add/update the operator count in the map 
    def visitor(op: PigOperator): Unit = {
      val lineage = op.lineageSignature

      op.outputs.flatMap(_.consumer).foreach{c => 
        val key = s"${lineage}-${c.lineageSignature}"
        
        map(key) += 1
      }
    }

    // traverse all plans and visit each operator within a plan
    schedule.foreach { plan => BreadthFirstTopDownWalker.walk(plan._1)(visitor) }

    
    // after we counted the edges for all plans, write the data back to file
    Files.write(file, 
        map.toIterable.map{ case (key,value) => s"${key}${delim}${value}"}.asJava, 
        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
  }
  
  
  lazy val url = if(Conf.statServerURL.isDefined) {
      Conf.statServerURL.get.toURI()
    } else {
      val addr = java.net.InetAddress.getLocalHost.getHostAddress
      logger.debug(s"identified local address as $addr")
      val u = java.net.URI.create(s"http://${addr}:${Conf.statServerPort}/")
      u
    }
}
