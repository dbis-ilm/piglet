package dbis.piglet.mm

import java.nio.file.{Files, Path, StandardOpenOption}

import dbis.piglet.Piglet.Lineage
import dbis.piglet.op.{PigOperator, TimingOp}
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.tools.logging.PigletLogging
import dbis.piglet.tools.{BreadthFirstTopDownWalker, CliParams, Conf, DepthFirstTopDownWalker}
import dbis.setm.SETM.timing
import org.json4s.native.Serialization.{read, write => swrite}

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutableMap}
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

case class Partition(lineage: String, partitionId: Int)

object DataflowProfiler extends PigletLogging {

  private[mm] var opCount: OpCount = _

  private val cache = MutableMap.empty[String, MaterializationPoint]

  private val exectimes = MutableMap.empty[String, T]

  val parentPartitionInfo = MutableMap.empty[String, MutableMap[Int, Seq[Seq[Int]]]]
  val currentTimes = MutableMap.empty[Partition, Long]

  // list of parent operators for each operator
  val parentsOf = MutableMap.empty[String, List[String]].withDefault(_ => List.empty)


  // for json (de-)serialization
  implicit val formats = org.json4s.native.Serialization.formats(org.json4s.NoTypeHints)

  override def toString = cache.mkString(",")

  def load(base: Path) = {
    logger.debug("init Dataflow profiler")

    exectimes.clear()

    val opCountFile = base.resolve(Conf.opCountFile)
    if(Files.exists(opCountFile)) {

      val json = Source.fromFile(opCountFile.toFile).getLines().mkString("\n")
      val opcounts = read[Map[Lineage, Map[Lineage, Int]]](json)

      if(opcounts.nonEmpty) {

        // copy content that was read from JSON to a _mutable_ map!
        val mm = MutableMap.empty[Lineage, MutableMap[Lineage, Int]]
        opcounts.filterKeys(_ != "meta").foreach{ case (parent,children) =>

          val mm2 = MutableMap.empty[Lineage, Int]
          children.foreach{ case (l,c) => mm2.put(l,c)}

          mm.put(parent, mm2)
        }
        // create opcount instance
        opCount = OpCount(opcounts("meta")("runs"), mm)
      } else
        opCount = OpCount.empty

    } else
      opCount = OpCount.empty

    val execTimesFile = base.resolve(Conf.execTimesFile)
    if(Files.exists(execTimesFile)) {
      val json = Source.fromFile(execTimesFile.toFile).getLines().mkString("\n")
      val exectimes1 = read[Map[String,T]](json)
      exectimes1.foreach( mapping => exectimes += mapping)
      logger.debug(s"loaded ${exectimes.size} execution time statistics")
    }
  }

  def reset() = {
    parentsOf.clear()
    currentTimes.clear()
  }

  def analyze(plan: DataflowPlan): Unit = timing("analyze plan") {

    // reset old values for parents and execution time
    reset()

    opCount.incrRuns()

    /* walk over the plan and
     *   - count operator occurrences
     *   - save the parent information
     */
    BreadthFirstTopDownWalker.walk(plan){ op =>
        val lineage = op.lineageSignature
        op.outputs.flatMap(_.consumer).foreach{ c =>
            opCount.add(lineage, c.lineageSignature)
        }

        val inputs = op.inputs.map(_.producer.lineageSignature)
        val p = if(inputs.nonEmpty) lineage -> inputs else lineage -> List("start")
        parentsOf += p
    }
  }


  def collect = {

    currentTimes//.keySet
                //.map(_.lineage)
                .filterNot{ case (Partition(lineage,_),_) => lineage == "start" || lineage == "end" || lineage == "progstart" }
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
      val parentTimes = parentPartitionIds(partitionId).zipWithIndex.flatMap{ case (list, idx) =>
          val parentLineage = parentLineages(idx)

          list.map{ pId =>
            val p = if(parentLineage == "start")
                Partition(parentLineage, -1) // for "start" we only have one value with partition id -1
//            else if(parentLineage == "progstart")
//              Partition(parentLineage, -1)
              else
                Partition(parentLineage,pId)

            if(currentTimes.contains(p))
              currentTimes(p)
            else {
              logger.error("currentTimes: ")
              logger.error(currentTimes.mkString("\n"))
              throw ProfilingException(s"no $p in list of current execution times")
            }
          }
        }



      val earliestParentTime = if(parentTimes.nonEmpty) {
        parentTimes.max
      } else {
        throw ProfilingException(s"no parent time for $lineage on partition $partitionId")
      }

      val duration = time - earliestParentTime

      val oldT = exectimes.getOrElse(lineage, T(0L, cnt = 0))
      val a = T.merge(oldT, duration) //T(oldT.cnt + 1, oldT.sum + duration)

      exectimes.update(lineage, a)
    }

    // manually add execution time for creating spark context
    val progStart = currentTimes(Partition("progstart", -1))
    val start = currentTimes(Partition("start", -1))
    exectimes += "sparkcontext" -> T(start - progStart)
    
//    logger.debug(s"total: ${currentTimes("end").head._2 - currentTimes("start").head._2}")
    
    exectimes.size
  }
      


  def addExecTime(lineage: String, partitionId: Int, parentPartitions: Seq[Seq[Int]], time: Long) = {

    val p = Partition(lineage, partitionId)
    if(currentTimes.contains(p)) {
      logger.warn(s"we already have a time for $p : oldTime: ${currentTimes(p)}  newTime: $time  (diff: ${currentTimes(p) - time}ms")
    }
    currentTimes +=  p -> time

    if(parentPartitionInfo.contains(lineage)) {

      val m = parentPartitionInfo(lineage)

      if(m.contains(partitionId)) {
        logger.warn(s"we already have that partition: $lineage  $partitionId . ")
      } else {
        m += partitionId -> parentPartitions
      }

    } else {
      parentPartitionInfo += lineage -> MutableMap(partitionId -> parentPartitions)
    }

  }
  
  def getExectime(op: String): Option[T] = exectimes.get(op)

  def writeStatistics(c: CliParams): Unit = {

    val execTimesFile = c.profiling.get.resolve(Conf.execTimesFile)

    // create json from collected times
    val json = swrite(exectimes)

    // overwrite old file
    Files.write(execTimesFile,
      List(json).asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)

    val opCountFile = c.profiling.get.resolve(Conf.opCountFile)
    logger.debug(s"writing opcounts to $opCountFile with ${opCount.size} entries")

    val o = Map(("meta",Map(("runs",opCount.totalRuns))))
    val opJson = swrite(opCount.adj ++ o)
    Files.write(opCountFile,
      List(opJson).asJava,
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

    DepthFirstTopDownWalker.walk(plan) {
      case _ : TimingOp => // we want to ignore Timing operators
      case op: PigOperator =>

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
          0L // TODO: calculate saving time at this point
        )

        this.addMaterializationPoint(mp)

      } else {
        logger.debug(s" found no runtime information")
      }
    }
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

case class ProfilingException(msg: String) extends Exception
