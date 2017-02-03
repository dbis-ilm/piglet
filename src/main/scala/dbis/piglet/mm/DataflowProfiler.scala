package dbis.piglet.mm

import scala.collection.mutable.{Map => MutableMap,ListBuffer}
import scala.collection.JavaConverters._
import scala.io.Source

import java.net.URI
import java.nio.file.Path
import java.nio.file.Files
import java.nio.file.StandardOpenOption

import dbis.piglet.op.PigOperator
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.plan.MaterializationManager
import dbis.piglet.tools.Conf
import dbis.piglet.tools.BreadthFirstTopDownWalker
import dbis.piglet.tools.DepthFirstTopDownWalker
import dbis.piglet.tools.logging.PigletLogging
import dbis.setm.SETM.timing
import dbis.piglet.tools.CliParams

/**
 * Created by kai on 24.08.15.
 */
object DataflowProfiler extends PigletLogging {

  logger.debug(s"using profiling ")
  
  val delim = ";"
  
  private val cache = MutableMap.empty[String, MaterializationPoint]
  
  /*private[mm]*/ val exectimes = MutableMap.empty[String, MutableMap[Int, ListBuffer[Long]]] //.withDefault(_ => MutableMap.empty[Int, ListBuffer[Long]].withDefault(_ => ListBuffer.empty[Long])) 
  private[mm] val opcounts  = MutableMap.empty[String, Int].withDefaultValue(0)

  override def toString = cache.mkString(",")

  def init(base: Path) = {
    val opCountFile = base.resolve(Conf.opCountFile)
    if(Files.exists(opCountFile)) {
      Source.fromFile(opCountFile.toFile()).getLines().foreach{line =>
        val arr = line.split(delim)
        val key = arr(0)
        val value = arr(1).toInt
        
        opcounts += (key -> value)
      }
    }  
    
    val execTimesFile = base.resolve(Conf.execTimesFile)
    if(Files.exists(execTimesFile)) {
      Source.fromFile(execTimesFile.toFile()).getLines().foreach{line =>
        val arr = line.split(delim)
        val lineage = arr(0)
        val partitionIdTimes = arr(1).split(":")
        val partitionId = partitionIdTimes(0).toInt
        
        val times = partitionIdTimes(1).split(",").map(_.toLong)
        
        exectimes(lineage)(partitionId) = times.to
      }
    } 
    
  }
  
  def writeStatistics(c: CliParams) = {
    
    val execTimesFile = c.profiling.get.resolve(Conf.execTimesFile)
    
    val a = exectimes.map{ case (lineage, idTimes) => 
      val b = idTimes.map{ case (partitionId, times) => s"${partitionId}:${times.mkString(",")}"} 
      s"$lineage$delim$b"
    }
    
    Files.write(execTimesFile, 
//        exectimes.map(t => s"${t._1}${delim}${t._2}${delim}${t._3}").toIterable.asJava,
        a.toIterable.asJava,
        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
    
    val opCountFile = c.profiling.get.resolve(Conf.opCountFile)
    Files.write(opCountFile, 
        opcounts.toIterable.map{ case (key,value) => s"${key}${delim}${value}"}.asJava, 
        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
  }
  
  def addExecTime(lineage: String, partitionId: Int, time: Long) = {
//    exectimes(lineage)(partitionId) += time
    
    if(exectimes.contains(lineage)) {
      val partitionTimes = exectimes(lineage)
      if(partitionTimes.contains(partitionId))
        partitionTimes(partitionId) += time
      else
        partitionTimes += (partitionId -> ListBuffer(time))
    } else {
      val m = MutableMap( (partitionId -> ListBuffer(time)) )
      exectimes += (lineage -> m)
    }
  }
  
  def getExectime(op: String, parents: String*): Option[Double] = {
    
    if(parents.size == 1) {
      
      val parent = parents.head
      val parentTimes = exectimes(parent)
      if(parentTimes.isEmpty) {
        logger.warn(s"no exectimes entry for parent lineage $parent")
//        throw new IllegalArgumentException(s"no exectimes entry for parent lineage $parent")
        None
      }
        
      val opTimes = exectimes(op)
      if(opTimes.isEmpty) {
        logger.warn(s"no exectimes entry for op lineage $op")
//        throw new IllegalArgumentException(s"no exectimes entry for op lineage $op")
        None
      }
      
      
      
      val avgTime = if(parentTimes.size == opTimes.size) {
      	val t = opTimes.map{ case (partitionId, oTimes) =>
      	  val pTimes = parentTimes(partitionId)
      	  
      	  val diffs = oTimes.zip(pTimes).map{case (c,p) => c - p}
      	  diffs.sum / diffs.size.asInstanceOf[Double] // average of all runs for same partition
      	}.toList
      	
      	t.sum / t.size.asInstanceOf[Double] // average of all partitions 
      	
      } else { // different number of in- and output partitions

        /* if there are different number of partitions for in- and output
         * we are very pessimistic and take the earliest start
         * time of the parent and the latest finish time of the op
         */
        val parentMins = parentTimes.values.transpose.map(_.min)
        val opMaxs = opTimes.values.transpose.map(_.max).toList
        
        val diffs = opMaxs.zip(parentMins).map{case (o,p) => o - p}
        diffs.sum / diffs.size.asInstanceOf[Double]
        
      }
      
      Some(avgTime)
    } else { // we have more than one parent
      
      /* if there is more than parent operator, we are also pessimistic
       *  
       */
      
//      val a = parents.map(exectimes(_).values.transpose.map(_.min)).min
      
      None
    }
     
  } 
  
  def getExectimes(lineage: String): Map[Int, List[Long]] = exectimes(lineage).toMap.map{ case (k,v) => (k,v.toList)}

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
  
  /**
   * Go over the plan and and check for existing runtime information for each op (lineage).
   * If something exists, create a materialization point that can be used later. 
   */
  def addMaterializationPoints(plan: DataflowPlan) = timing("identify mat points") {
    
    DepthFirstTopDownWalker.walk(plan) { op =>

      logger.debug( s"""checking storage service for runtime information for operator "${op.lineageSignature}" """)
      // check for the current operator, if we have some runtime/stage information 
      val avg: Option[(Long, Long)] = None// getExectimes(op.lineageSignature) 

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
  def createOpCounter(schedule: Seq[(DataflowPlan, Path)], c: CliParams) = timing("analyze plans") {

    logger.debug("start creating lineage counter map")
    
    // the visitor to add/update the operator count in the map 
    def visitor(op: PigOperator): Unit = {
      val lineage = op.lineageSignature

      op.outputs.flatMap(_.consumer).foreach{c => 
        val key = s"${lineage}-${c.lineageSignature}"
        
        opcounts(key) += 1
      }
    }

    // traverse all plans and visit each operator within a plan
    schedule.foreach { plan => BreadthFirstTopDownWalker.walk(plan._1)(visitor) }
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