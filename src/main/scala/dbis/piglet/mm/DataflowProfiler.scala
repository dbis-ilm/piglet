package dbis.piglet.mm

import java.net.URI
import java.nio.file.{Files, Path, StandardOpenOption}

import dbis.piglet.Piglet.Lineage
import dbis.piglet.mm.DuplicateStrategy.DuplicateStrategy
import dbis.piglet.op.CacheMode.CacheMode
import dbis.piglet.op.{CacheMode, Empty, Load, TimingOp}
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.tools.logging.PigletLogging
import dbis.piglet.tools.{BreadthFirstTopDownWalker, CliParams, Conf}
import dbis.setm.SETM.timing

import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, Map => MutableMap}
import scala.concurrent.duration._
import scala.io.Source


case class T private(sum: Long, cnt: Int, min: Long = Long.MaxValue, max: Long = Long.MinValue) {
  def avg(): Long = sum / cnt
}

object T {

  def apply(value: Long): T = T(value,1,value,value)

  def merge(t: T, value: Long): T = T(cnt = t.cnt +1,
                                      sum = t.sum + value,
                                      min = if(value < t.min) value else t.min,
                                      max = if(value > t.max) value else t.max)
}

case class Partition(lineage: Lineage, partitionId: Int) extends Ordered[Partition] {
  override def compare(that: Partition) =
    if(lineage < that.lineage)
      -1
    else if(lineage > that.lineage)
      1
    else {
      if(partitionId < that.partitionId)
        -1
      else if(partitionId > that.partitionId)
        1
      else
        0
    }
}
case class SizeInfo(lineage: Lineage, records: Long, bytes: Double)
case class TimeInfo(lineage: Lineage, partitionId: Int, time: Long, parentPartitions: List[List[Int]])

object DataflowProfiler extends PigletLogging {


  protected[mm] var profilingGraph: Markov = _

  val parentPartitionInfo = MutableMap.empty[Lineage, MutableMap[Int, Seq[Seq[Int]]]]
  val currentTimes = MutableMap.empty[Partition, Long]

  // for json (de-)serialization
//  implicit val formats = org.json4s.native.Serialization.formats(org.json4s.NoTypeHints)

  private val alreadyExistingMsgs = ListBuffer.empty[(Partition,Long,Long)]


  def load(base: Path) = {
    logger.debug("init Dataflow profiler")

    val profilingFile = base.resolve(Conf.profilingFile)
    if(Files.exists(profilingFile)) {
      val json = Source.fromFile(profilingFile.toFile).getLines().mkString("\n")
      profilingGraph = Markov.fromJson(json)
    } else
      profilingGraph = Markov.empty

    logger.debug(s"loaded markov model with size: ${profilingGraph.size}")
    logger.debug(s"total runs in markov is: ${profilingGraph.totalRuns}")
  }

  def reset() = {
    currentTimes.clear()
  }

  /**
    * Analyze the current plan.
    *
    * This will count the operator counts and number of transitions from Op A to Op B etc
    * @param plan The plan to analyze
    * @return Returns the updated model (Markov chain) that contains operator statistics from
    *         previous runs as well as the updated counts
    */
  def analyze(plan: DataflowPlan): Markov = timing("analyze plan") {

    // reset old values for parents and execution time
    reset()

    profilingGraph.incrRuns()

    /* walk over the plan and
     *   - count operator occurrences
     *   - save the parent information
     */

    BreadthFirstTopDownWalker.walk(plan){
      case _: TimingOp => // ignore timing operators
      case op =>
        val lineage = op.lineageSignature

        if(op.isInstanceOf[Load])
          profilingGraph.add(Markov.startLineage,lineage)

        op.outputs.flatMap(_.consumer).withFilter{
          case _: Empty => false
          case _: TimingOp => false
          case _ => true
        }
        .foreach{ c =>
          logger.debug(s"add to model ${op.name} -> ${c.name}")
          profilingGraph.add(lineage, c.lineageSignature)
        }
    }

    profilingGraph
  }


  def collect() = timing("process runtime stats") {

    if(alreadyExistingMsgs.nonEmpty) {
//      alreadyExistingMsgs.sortBy(_._1).foreach{ case (p,time, diff) =>
//        logger.warn(s"we already have a time for $p : oldTime: ${currentTimes(p)}  newTime: $time (diff: $diff ms)  - Strategy: ${CliParams.values.profiling.get.duplicates}")
//      }
      logger.warn(s"found duplicate times - use strategy ${CliParams.values.profiling.get.duplicates}")
      alreadyExistingMsgs.groupBy(_._1.lineage).map{ case (lineage, values) => (lineage, values.length, values.map(_._1.partitionId).min, values.map(_._1.partitionId).max)}.foreach { case (lineage, num, min, max) =>
        logger.warn(s"  $lineage : min = $min, max = $max, num = $num partitions")
      }
    }

    val noParentMsgs = ListBuffer.empty[(Lineage, Int)]


    currentTimes//.keySet
                //.map(_.lineage)
                .filterNot{ case (Partition(lineage,_),_) => lineage == Markov.startLineage || lineage == "end" || lineage == "progstart" }
                .foreach{ case (partition,time) =>

      val lineage = partition.lineage
      val partitionId = partition.partitionId

      // parent operators
      val parentLineages = profilingGraph.parents(lineage).getOrElse(List(Markov.startLineage))

      // list of parent partitions per parent
      val parentPartitionIds = parentPartitionInfo(lineage)


      /* for each parent operator, get the times for the respective parent partitions.
       * and at the end take the min (first processed parent partition) or max (last processed parent partition) value
       */
      val parentTimes = getEarliestParentTimes(partitionId, parentPartitionIds, parentLineages)

      val earliestParentTime = if(parentTimes.nonEmpty) {
        parentTimes.max
      } else {
//        throw ProfilingException(s"no parent time for $lineage on partition $partitionId")
//        logger.debug(s"no parent time for $lineage on partition $partitionId")
        noParentMsgs += ((lineage, partitionId))
        time + 1
      }

      val duration = time - earliestParentTime

      profilingGraph.updateCost(lineage, duration)

    }

    noParentMsgs.groupBy(_._1)
                .map{ case (lineage, l) => (lineage, l.map(_._2).sorted) }
                .foreach{ case (lineage, lst) =>
                  logger.debug(s"no parent time for $lineage on partition ${lst.mkString(",")} (${lst.length})")
                }


    // manually add execution time for creating spark context
    val progStart = currentTimes(Partition("progstart", -1))
    val start = currentTimes(Partition("start", -1))

    profilingGraph.add("sparkcontext","start")
    profilingGraph.updateCost("sparkcontext", start - progStart)

  }


  private def getEarliestParentTimes(partitionId: Int, parentPartitionIds: MutableMap[Int, Seq[Seq[Int]]], parentLineages: List[Lineage]) =

    parentPartitionIds(partitionId).zipWithIndex.flatMap { case (parentPartitionsOfCurrentPartition, idx) =>

      val parentLineage = parentLineages(idx)
      val parentMaxPartitionId = parentPartitionIds.valuesIterator.flatMap(_.flatten).max

      parentPartitionsOfCurrentPartition.map { pId =>


        val p = if (parentLineage == Markov.startNode.lineage)
            Partition(parentLineage, -1) // for "start" we only have one value with partition id -1
          else {
            val theParentId = if (pId > parentMaxPartitionId) {
              val i = parentPartitionIds.valuesIterator.flatMap(_.flatten).map(parentId => (parentId, math.abs(parentId - pId))).toList.minBy(_._2)._1
              logger.warn(s"partition id is out of range ($pId > $parentMaxPartitionId) - will use $i")
              i
            } else pId
            Partition(parentLineage, theParentId)
          }

        if (currentTimes.contains(p))
          currentTimes(p)
        else {
          //              logger.error("currentTimes: ")
          //              val sortedTimes = currentTimes.toList.sortWith{ (l,r) =>
          //                val (Partition(leftLineage, leftPartId),_) = l
          //                val (Partition(rightLineage, rightPartId),_) = r
          //
          //                val comp = leftLineage.compareTo(rightLineage)
          //
          //                if(comp < 0)
          //                  true
          //                else if(comp == 0)
          //                  leftPartId < rightPartId
          //                else
          //                  false
          //              }
          //              logger.error(sortedTimes.mkString("\n"))


//          val msg = s"no $p in list of current execution times"
//          logger.warn(msg)
          //              throw ProfilingException(s"no $p in list of current execution times (as parent for $partition)")
          -1L
        }
      }
    }.filter(_ >= 0)

//  def addExecTime(lineage: Lineage, partitionId: Int, parentPartitions: List[List[Int]], time: Long): Unit = {
  def addExecTime(tInfo: TimeInfo): Unit = {

    val p = Partition(tInfo.lineage, tInfo.partitionId)
    if(currentTimes.contains(p)) {
      val ps = CliParams.values.profiling.get
      alreadyExistingMsgs += ((p, tInfo.time, currentTimes(p) - tInfo.time))


      val oldTime = currentTimes(p)

      /*
       Update the time in currentTimes only if the newly arrived time matches the strategy
       */
      ps.duplicates match {
        case DuplicateStrategy.NEWEST if oldTime < tInfo.time =>
          currentTimes.update(p, tInfo.time)

        case DuplicateStrategy.OLDEST if tInfo.time < oldTime =>
          currentTimes.update(p, tInfo.time)

        case _ => // intentionally left empty
      }
    } else {
      currentTimes +=  p -> tInfo.time
    }



    if(parentPartitionInfo.contains(tInfo.lineage)) {

      val m = parentPartitionInfo(tInfo.lineage)

      if(m.contains(tInfo.partitionId)) {
//        logger.warn(s"we already have that partition: $lineage  $partitionId . ")
      } else {
        m += tInfo.partitionId -> tInfo.parentPartitions
      }

    } else {
      parentPartitionInfo += tInfo.lineage -> MutableMap(tInfo.partitionId -> tInfo.parentPartitions)
    }

  }


  def addSizes(m: Array[SizeInfo], factor: Int) = {
    m.foreach{
      case SizeInfo(lineage, records, bytes) =>
        val bytesPerRecord = bytes // / records
        val totalRecords = records * factor

        logger.debug(s"add size info for $lineage : records= $records\t* $factor = $totalRecords,\tbpr = $bytesPerRecord")

        profilingGraph.updateSize(SizeInfo(lineage, records = totalRecords, bytes = bytesPerRecord))
    }
    logger.debug(s"added size info for ${m.length} operators")
  }

  def getExectime(op: Lineage): Option[T] = profilingGraph.cost(op)

  def writeStatistics(): Unit = {

    val opCountFile = Conf.programHome.resolve(Conf.profilingFile)
    logger.debug(s"writing opcounts to $opCountFile with ${profilingGraph.size} entries")

    val opJson = profilingGraph.toJson
    Files.write(opCountFile,
      List(opJson).asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  def opInputSize(op: Lineage) = profilingGraph.resultRecords(op)

}

case class ProfilingException(msg: String) extends Exception


object ProbStrategy extends Enumeration  {
  type ProbStrategy = Value
  val MIN, MAX, AVG, PRODUCT = Value

  def func(s: ProbStrategy): (Traversable[Double]) => Double = s match {
    case MIN => Markov.ProbMin
    case MAX => Markov.ProbMax
    case AVG => Markov.ProbAvg
    case PRODUCT => Markov.ProbProduct
  }
}

object CostStrategy extends Enumeration {
  type CostStrategy = Value
  val MIN, MAX = Value

  def func(s: CostStrategy): (Traversable[(Long, Double)]) => (Long, Double) = s match {
    case MIN => Markov.CostMin
    case MAX => Markov.CostMax
  }
}

object DuplicateStrategy extends Enumeration {
  type DuplicateStrategy = Value
  val NEWEST, OLDEST = Value
}

import dbis.piglet.mm.CostStrategy.CostStrategy
import dbis.piglet.mm.ProbStrategy.ProbStrategy

case class ProfilerSettings(
                             minBenefit: Duration = Conf.mmDefaultMinBenefit,
                             probThreshold: Double = Conf.mmDefaultProbThreshold,
                             costStrategy: CostStrategy = Conf.mmDefaultCostStrategy,
                             probStrategy: ProbStrategy = Conf.mmDefaultProbStrategy,
                             cacheMode: CacheMode = Conf.mmDefaultCacheMode,
                             fraction: Int = Conf.mmDefaultFraction,
                             duplicates: DuplicateStrategy = DuplicateStrategy.NEWEST,
                             url: URI = ProfilerSettings.profilingUrl
                           )

object ProfilerSettings extends PigletLogging {
  def apply(m: Map[String, String]): ProfilerSettings = {
    var ps = ProfilerSettings()

    m.foreach { case (k,v) =>
      k match {
        case "prob" => ps = ps.copy(probThreshold = v.toDouble)
        case "benefit" => ps = ps.copy(minBenefit = v.toDouble.seconds)
        case "cost_strategy" => ps = ps.copy(costStrategy = CostStrategy.withName(v.toUpperCase))
        case "prob_strategy" => ps = ps.copy(probStrategy = ProbStrategy.withName(v.toUpperCase))
        case "cache_mode" => ps = ps.copy(cacheMode = CacheMode.withName(v.toUpperCase))
        case "fraction" => ps = ps.copy(fraction = v.toInt)
        case "duplicates" => ps = ps.copy(duplicates = DuplicateStrategy.withName(v.toUpperCase))
        case _ => logger warn s"unknown profiler settings key $k (value: $v) - ignoring"
      }
    }
    ps
  }

  private lazy val profilingUrl = Conf.statServerURL.getOrElse {
    val addr = java.net.InetAddress.getLocalHost.getHostAddress
    logger.debug(s"identified local address as $addr")
    val u = URI.create(s"http://$addr:${Conf.statServerPort}/")
    u
  }
}