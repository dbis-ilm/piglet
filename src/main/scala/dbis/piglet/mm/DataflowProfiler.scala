package dbis.piglet.mm

import scala.collection.mutable.{Map => MutableMap,ListBuffer}
import scala.collection.JavaConverters._
import scala.io.Source

import java.net.URI
import java.nio.file.Path
import java.nio.file.Files
import java.nio.file.StandardOpenOption

import org.json4s.native.Serialization.{read, write => swrite}

import dbis.piglet.op.PigOperator
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.plan.MaterializationManager
import dbis.piglet.tools.Conf
import dbis.piglet.tools.BreadthFirstTopDownWalker
import dbis.piglet.tools.DepthFirstTopDownWalker
import dbis.piglet.tools.logging.PigletLogging
import dbis.setm.SETM.timing
import dbis.piglet.tools.CliParams
import dbis.piglet.op.TimingOp

case class T(cnt: Int, sum: Long)

/**
 * Created by kai on 24.08.15.
 */
object DataflowProfiler extends PigletLogging {

  val delim = ";"
  
  private val cache = MutableMap.empty[String, MaterializationPoint]
  
  private val exectimes = MutableMap.empty[String, T]
  val currentTimes = MutableMap.empty[String, ListBuffer[(Int, Long)]]
  
  private[mm] val opcounts  = MutableMap.empty[String, Int].withDefaultValue(0)
  
  val parents = MutableMap.empty[String, List[String]].withDefault(_ => List.empty)
  
  
  // for json (de-)serialization
  implicit val formats = org.json4s.native.Serialization.formats(org.json4s.NoTypeHints)
  
  override def toString = cache.mkString(",")

  def init(base: Path, plan: DataflowPlan) = {
    logger.debug("init Dataflow profiler")
    
    exectimes.clear()
    opcounts.clear()
    parents.clear()
    currentTimes.clear()
    
    plan.operators.filterNot(_.isInstanceOf[TimingOp]).foreach{op =>
      parents += (op.lineageSignature -> op.inputs.map(_.producer.lineageSignature))
      println(s"$op --> ${parents(op.lineageSignature)}")
    }
    
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
      val json = Source.fromFile(execTimesFile.toFile()).getLines().mkString("\n")
      val exectimes1 = read[Map[String,T]](json)
      exectimes1.foreach( mapping => exectimes += mapping)
      logger.debug(s"loaded ${exectimes.size} execution time statistics")
    } 
  }
  
  def collect = {
    currentTimes.foreach{ case (lineage, times) =>
      currentTimes.update(lineage, times.sortBy(_._2)) // sort by time
    }
    
    currentTimes.filterNot{ case (id, _) => id == "start" || id == "end" }.foreach{ case (lineage, times) =>
      
      val parentsOps = parents(lineage) 
        
      val parentTimes = if(parentsOps.size == 1) {
        currentTimes(parentsOps.head)
      } else {
        parentsOps.flatMap(currentTimes(_))
      }
        
      val duration = if(parentTimes.size == times.size) {
        val sum = parentTimes.map(_._2) // times of parent operator
                             .zip(times.map(_._2)) // zip with times of current op
                             .map{ case (p,c) => // calculate time difference between parent and self 
//                                 if(c < p)
//                                   throw new IllegalStateException(s"child partition could not be started before parent partition: child: $c  parent: $p")
                               math.abs(c - p) 
                             } 
                             .sum // sum differences
        
        sum
        
      } else { // parent/parents has different number of partitions than current op
        
                
        val ratio = parentTimes.size.asInstanceOf[Double] / times.size 
        println(s"op: $lineage  parents: ${parentTimes.size}  op = ${times.size} ratio = $ratio")
        
        
    	  val opFinish = times.last._2
        
    	  val parentFinishFirst = if(parentTimes.nonEmpty) parentTimes.map(_._2).min else currentTimes("start").head._2
  
        val opDuration = opFinish - parentFinishFirst
        opDuration        
      }
      
      val oldT = exectimes.getOrElse(lineage, T(0,0L))
      val a = T(oldT.cnt + 1, oldT.sum + duration)

      exectimes.update(lineage, a)
    }
    
    exectimes.size
  }
      
  
  def writeStatistics(c: CliParams) = {
    
    val execTimesFile = c.profiling.get.resolve(Conf.execTimesFile)

    val json = swrite(exectimes)
    
    Files.write(execTimesFile, 
        List(json).toIterable.asJava,
        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
    
    val opCountFile = c.profiling.get.resolve(Conf.opCountFile)
    Files.write(opCountFile, 
        opcounts.toIterable.map{ case (key,value) => s"${key}${delim}${value}"}.asJava, 
        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
  }
  
  def addExecTime(lineage: String, partitionId: Int, time: Long) = {
    if(currentTimes.contains(lineage)) {
      currentTimes(lineage) += ((partitionId, time))
    } else {
      currentTimes += (lineage -> ListBuffer((partitionId, time)))
    }
  }
  
  def getExectime(op: String): Option[Double] = {
    val s = if(exectimes.contains(op)) {
      val T(cnt,sum) = exectimes(op)
      Some(sum / cnt.asInstanceOf[Double])
    } else
      None
      
//    println(s"returning $s")  
    s 
  } 
  
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
