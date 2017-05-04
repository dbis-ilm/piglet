package dbis.piglet.mm

import java.nio.file.{Files, Path, StandardOpenOption}

import dbis.piglet.op.{PigOperator, TimingOp}
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.tools.logging.PigletLogging
import dbis.piglet.tools.{BreadthFirstTopDownWalker, CliParams, Conf, DepthFirstTopDownWalker}
import dbis.setm.SETM.timing
import org.json4s.native.Serialization.{read, write => swrite}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, Map => MutableMap}
import scala.io.Source

case class T(cnt: Int, sum: Long)


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

object DataflowProfiler extends PigletLogging {

  implicit def toMedianList[T](l: Seq[T]): MedianList[T] = MedianList(l)
  implicit def toAvgList[T <: Long](l: Seq[T]): AvgList = AvgList(l)
  
  val delim = ";"
  
  private val cache = MutableMap.empty[String, MaterializationPoint]
  
  private val exectimes = MutableMap.empty[String, T]
  val currentTimes = MutableMap.empty[String, ListBuffer[(Int, Long, Seq[Seq[Int]])]]
  
  private[mm] val opcounts  = MutableMap.empty[String, Int].withDefaultValue(0)
  
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
//    currentTimes.foreach{ case (lineage, times) =>
//      currentTimes.update(lineage, times.sortBy(_._2)) // sort by time
//    }

    currentTimes.filterNot{ case (lineage, _) => lineage == "start" || lineage == "end" }
                .foreach{ case (lineage, times) =>


      val parents = parentsOf(lineage)
        
      val parentTimes: Seq[ListBuffer[(Int, Long, Seq[Seq[Int]])]] =
        parents.map(currentTimes(_))


      times.zipWithIndex.foreach { case ((_, time, parentPartitions),parentIdx) =>

          val t = parentTimes(parentIdx)
          val parentPartitionTimes = parentPartitions(parentIdx)



      }

      val duration: Long = 0

//      val duration = if(parentTimes.size == times.size) {
//        val sum = parentTimes.map(_._2) // times of parent operator
//                             .zip(times.map(_._2)) // zip with times of current op
//                             .map{ case (p,c) => // calculate time difference between parent and self
////                                 if(c < p)
////                                   throw new IllegalStateException(s"child partition could not be started before parent partition: child: $c  parent: $p")
//                               math.abs(c - p)
//                             }
//                             .sum // sum differences
//
//        sum
//
//      } else { // parent/parents has different number of partitions than current op
//
//        val ratio = parentTimes.size.asInstanceOf[Double] % times.size
//        println(s"op: $lineage  parents: ${parentTimes.size} (${parents(lineage).size})  op = ${times.size} ratio = $ratio")
//
//        if(ratio == 0 && parents.size > 1) {
//
//          /* we implement three different approaches to calculate the elapsed time of an operator.
//           *
//           * 1.: median: determine the median time of all parent partitions - and also of the Op's partitions
//           * 2.: avg: average time of parent partitions - and Op's partitions
//           * 3.: earliest time for each partition (by Id)
//           */
//
//          // median
//          val medianParent = parentTimes.map(_._2).sortBy(identity).median
//          val medianOp = times.map(_._2).sortBy(identity).median
//          medianOp - medianParent
//
////          // min avg of parents
////          val avgParent = parents.map(currentTimes(_).map(_._2)).map(_.avg).min
////          val avgOp = times.map(_._2).avg
////          (avgOp - avgParent).toLong
//
//          // this takes the earliest time of each partition - very pessimistic and not accurate
////          parents.map(currentTimes(_)/*.sortBy(_._1)*/.map(_._2)) // get times for parent
////                   .transpose // we had a list of times per parent, now we have a list of lists where the inner lists contain the time for the respective partition
////                   .map(_.min) // we are pessimistic at take the earliest starting time
////                   .zip(times.map(_._2)) // zip with the finish times of the operator
////                   .map{ case (p,c) => math.abs(c - p) } // calculate the differences
////                   .sum // sum the differences
//
//        } else if(ratio == 0 && parents.size == 1) {
//          // compute how many of the parent's partition will result in one of Ops partition
//          val c = math.max(parentTimes.size / times.size,1)
//          parentTimes.map(_._2)// get only times of parent
//            .grouped(c) //create partitions of size c
//            .map(_.min) // for every list, we only take the earliest time (pessimistic)
//            .zip(times.map(_._2).toIterator) // combine with op's times
//            .map{ case (p,c1) => math.abs(c1 - p) } // calculate the differences
//            .sum // sum the differences
//
//        } else {
//          logger.warn(s"using very pessimistic calculation for $lineage")
//      	  val opFinish = times.last._2
//
//      	  val parentFinishFirst = if(parentTimes.nonEmpty) parentTimes.map(_._2).min else currentTimes("start").head._2
//
//          val opDuration = opFinish - parentFinishFirst
//          opDuration
//        }
//      }


      val oldT = exectimes.getOrElse(lineage, T(0,0L))
      val a = T(oldT.cnt + 1, oldT.sum + duration)

      exectimes.update(lineage, a)
    }
    
    logger.debug(s"total: ${currentTimes("end").head._2 - currentTimes("start").head._2}")
    
    exectimes.size
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
  
  def addExecTime(lineage: String, partitionId: Int, parentPartitions: Seq[Seq[Int]], time: Long) = {
    if(currentTimes.contains(lineage)) {
      currentTimes(lineage) += ((partitionId, time, parentPartitions))
    } else {
      currentTimes += (lineage -> ListBuffer((partitionId, time, parentPartitions)))
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
