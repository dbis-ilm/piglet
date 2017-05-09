package dbis.piglet.mm

import java.nio.file.{Files, Path, StandardOpenOption}

import dbis.piglet.op.{PigOperator, TimingOp}
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.tools.logging.PigletLogging
import dbis.piglet.tools.{BreadthFirstTopDownWalker, CliParams, Conf, DepthFirstTopDownWalker}
import dbis.setm.SETM.timing
import org.json4s.native.Serialization.{read, write => swrite}

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutableMap}
import scala.io.Source

case class T(cnt: Int, sum: Long) {
  def value(): Long = sum / cnt
}


case class MedianList[T](list: Seq[T]) {
  def median: T = if(list.isEmpty) {
    throw new UnsupportedOperationException("median of empty list")
  } else if(list.size == 1)
    list.head
  else
    list(list.size / 2)
}

case class AvgList(list: Seq[Long]) {
  
  def avg: Double = if(list.isEmpty) 
    throw new UnsupportedOperationException("avg of empty list")
  else if(list.size == 1)
    list.head.toDouble
  else {
    list.sum / list.size.asInstanceOf[Double]
  }
    
}

case class Partition(lineage: String, partitionId: Int)

object DataflowProfiler extends PigletLogging {

  implicit def toMedianList[T](l: Seq[T]): MedianList[T] = MedianList(l)
  implicit def toAvgList[T <: Long](l: Seq[T]): AvgList = AvgList(l)
  
  val delim = ";"

  private[mm] val opcounts  = MutableMap.empty[String, Int].withDefaultValue(0)

  private val cache = MutableMap.empty[String, MaterializationPoint]

  private val exectimes = MutableMap.empty[String, T]

  // map lineage ->
//  val currentTimes = MutableMap.empty[String, ListBuffer[(Int, Long, Seq[Seq[Int]])]]

  val parentPartitionInfo = MutableMap.empty[String, MutableMap[Int, Seq[Seq[Int]]]]
  val currentTimes = MutableMap.empty[Partition, Long]

  // list of parent operators for each operator
  val parentsOf = MutableMap.empty[String, List[String]].withDefault(_ => List.empty)


  // for json (de-)serialization
  implicit val formats = org.json4s.native.Serialization.formats(org.json4s.NoTypeHints)

  override def toString = cache.mkString(",")

  def init(base: Path, plan: DataflowPlan) = {
    logger.debug("init Dataflow profiler")

    exectimes.clear()
    opcounts.clear()
    parentsOf.clear()
    currentTimes.clear()

    plan.operators.filterNot(_.isInstanceOf[TimingOp]).foreach{op =>
      val inputs = op.inputs.map(_.producer.lineageSignature)

      val p = if(inputs.nonEmpty) op.lineageSignature -> inputs else op.lineageSignature -> List("start")

      parentsOf += p
    }

    val opCountFile = base.resolve(Conf.opCountFile)
    if(Files.exists(opCountFile)) {
      Source.fromFile(opCountFile.toFile).getLines().foreach{ line =>
        val arr = line.split(delim)
        val key = arr(0)
        val value = arr(1).toInt

        opcounts += (key -> value)
      }
    }

    val execTimesFile = base.resolve(Conf.execTimesFile)
    if(Files.exists(execTimesFile)) {
      val json = Source.fromFile(execTimesFile.toFile).getLines().mkString("\n")
      val exectimes1 = read[Map[String,T]](json)
      exectimes1.foreach( mapping => exectimes += mapping)
      logger.debug(s"loaded ${exectimes.size} execution time statistics")
    }
  }

  def collect = {

    currentTimes//.keySet
                //.map(_.lineage)
                .filterNot{ case (Partition(lineage,_),_) => lineage == "start" || lineage == "end" }
                .foreach{ case (partition,time) =>

      val lineage = partition.lineage
      val partitionId = partition.partitionId

      // parent operators
      val parentLineages = parentsOf(lineage)
      // list of parent partitions per parent
      val parentPartitionIds = parentPartitionInfo(lineage)

      /* for each parent operator, get the times for the respective parent partitions.
       * and at the end take the min (first processed parent partition) or max (last processed parent partition) value
       */
      val earliestParentTime = parentPartitionIds(partitionId).zipWithIndex.flatMap{ case (list, idx) =>
          val parentLineage = parentLineages(idx)

          list.map{ pId =>
            val p = if(parentLineage == "start")
                Partition(parentLineage, -1) // for "start" we only have one value with partition id -1
              else
                Partition(parentLineage,pId)

            if(currentTimes.contains(p))
              currentTimes(p)
            else {
              logger.error(currentTimes.mkString("\n"))
//              sys.error(s"cannot find $p in current times")
              0
            }
          }
        }.max

      val duration = time - earliestParentTime

      val oldT = exectimes.getOrElse(lineage, T(0,0L))
      val a = T(oldT.cnt + 1, oldT.sum + duration)

      exectimes.update(lineage, a)
    }
    
//    logger.debug(s"total: ${currentTimes("end").head._2 - currentTimes("start").head._2}")
    
    exectimes.size
  }
      
  
  def addExecTime(lineage: String, partitionId: Int, parentPartitions: Seq[Seq[Int]], time: Long) = {
//    if(currentTimes.contains(lineage)) {
//      currentTimes(lineage) += ((partitionId, time, parentPartitions))
//    } else {
//      currentTimes += (lineage -> ListBuffer((partitionId, time, parentPartitions)))
//    }

    val p = Partition(lineage, partitionId)
    if(currentTimes.contains(p)) {
      logger.warn(s"we already have a time for $p : oldTime: ${currentTimes(p)}  newTime: $time  (diff: ${currentTimes(p) - time}ms")
    }
    currentTimes +=  p -> time

    if(parentPartitionInfo.contains(lineage)) {

      val m = parentPartitionInfo(lineage)

      if(m.contains(partitionId)) {
        // warning?
//        logger.warn(s"WARNING: we already have that partition: $lineage  $partitionId . ")
      } else {
        m += partitionId -> parentPartitions
      }

    } else {
      parentPartitionInfo += lineage -> MutableMap(partitionId -> parentPartitions)
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

  def writeStatistics(c: CliParams) = {

    val execTimesFile = c.profiling.get.resolve(Conf.execTimesFile)

    // create json from collected times
    val json = swrite(exectimes)

    // overwrite old file
    Files.write(execTimesFile,
      List(json).asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)

    val opCountFile = c.profiling.get.resolve(Conf.opCountFile)
    Files.write(opCountFile,
      opcounts.map{ case (key,value) => s"$key$delim$value"}.asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  def getMaterializationPoint(hash: String): Option[MaterializationPoint] = cache.get(hash)

  def addMaterializationPoint(matPoint: MaterializationPoint): Unit = {
    val entry = cache.getOrElse(matPoint.hash, matPoint)
    if (entry.parentHash.nonEmpty && cache.contains(entry.parentHash.get)) {
      // TODO: calculate _cumulative_ benefit
      val parent = cache(entry.parentHash.get)
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
        val key = s"$lineage-${c.lineageSignature}"
        
        opcounts(key) += 1
      }
    }

    // traverse all plans and visit each operator within a plan
    schedule.foreach { plan => BreadthFirstTopDownWalker.walk(plan._1)(visitor) }
  }
  
  
  lazy val url = if(Conf.statServerURL.isDefined) {
      Conf.statServerURL.get.toURI
    } else {
      val addr = java.net.InetAddress.getLocalHost.getHostAddress
      logger.debug(s"identified local address as $addr")
      val u = java.net.URI.create(s"http://$addr:${Conf.statServerPort}/")
      u
    }
}
