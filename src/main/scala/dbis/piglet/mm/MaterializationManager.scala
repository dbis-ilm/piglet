package dbis.piglet.mm
import java.net.URI
import java.nio.file.{Files, StandardOpenOption}

import dbis.piglet.Piglet.Lineage
import dbis.piglet.op._
import dbis.piglet.plan.rewriting.internals.ProfilingSupport
import dbis.piglet.plan.{DataflowPlan, OperatorNotFoundException, PipeNameGenerator}
import dbis.piglet.tools._
import dbis.piglet.tools.logging.PigletLogging
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.io.Source


object MaterializationManager extends PigletLogging {

  private val STORAGE_CLASS = "BinStorage"  //JsonStorage2

  def replaceWithLoad(materialize: PigOperator, path: URI, plan: DataflowPlan): DataflowPlan = {


    val newPlan = plan

    val consumers = materialize.inputs.head.producer // the producer is the only input of MATERIALIZE
                      .outputs.head.consumer // all consumers of the producer
                      .filterNot(_.isInstanceOf[Materialize]) // but without the MATERIALIZE op


    val p = Pipe(materialize.inPipeName)
    val loader = Load(p, path.toString, materialize.constructSchema, Some(STORAGE_CLASS))

    //    newPlan = newPlan.remove(materialize, removePredecessors = true)

    val opAncestors = BreadthFirstBottomUpWalker.collect(newPlan, Seq(materialize))
    newPlan.operators = newPlan.operators.filterNot{ o => o == materialize || opAncestors.contains(o)}

    newPlan.addOperator(Seq(loader), deferrConstruct = true)

    consumers.foreach { c =>
      c.inputs.filter(p => p.name == loader.outPipeName).head.producer = loader
      loader.addConsumer(loader.outPipeName, c)
    }

    materialize.inputs.head.producer.outputs = List.empty
//    logger.info(s"pipe: $p")
    loader.outputs = List(p)

    logger.info(s"plan ops: \n${newPlan.operators.map(_.name).mkString("\n")}")

    logger.debug(s"replaced materialize op with loader $loader")

    /* TODO: do we need to remove all other nodes that get disconnected now by hand
       * or do they get removed during code generation (because there is no sink?)
       */

    newPlan.constructPlan(newPlan.operators)
    newPlan
  }

  def replaceWithStore(materialize: Materialize, path: URI, plan: DataflowPlan): DataflowPlan = {

    var newPlan = plan

    val producer = materialize.inputs.head.producer

    val p = producer.outputs.filter(_.name == materialize.inputs.head.name).head
    val storer = Store(p, path.toString, Some(STORAGE_CLASS))

    newPlan.addOperator(Seq(storer))
    newPlan = newPlan.remove(materialize)
    newPlan = newPlan.insertAfter(producer, storer)

    logger.info(s"inserted new store operator $storer")

    newPlan
  }
}


/**
  * Manage where materialized intermediate results are stored
  */
class MaterializationManager(private val matBaseDir: URI) extends PigletLogging {

  implicit val formats = Serialization.formats(NoTypeHints)

  logger.debug(s"materialization base directory: $matBaseDir")
  //  logger.debug(s"using materialization storage service at $url")

  require(matBaseDir != null, "Base directory for materialization must not be null")


  /**
    * Already existing materializations
    *
    * They're read from file and stored as a mapping from lineage --> file name
    */
  var materializations: Map[Lineage, URI] = if (Files.exists(Conf.materializationMapFile)) {
    val json = Source.fromFile(Conf.materializationMapFile.toFile).getLines().mkString("\n")

    if(json.isEmpty)
      Map.empty[Lineage, URI]
    else
      parse(json).extract[Map[Lineage, String]].map{case(k,v) => (k,new URI(v))}

  } else {
    Map.empty[Lineage, URI]
  }


  /**
    * Checks the complete plan for potential materialization points
    * @param plan The plan
    * @param globalOpGraph The markov model that represents previously collected statistics
    * @return Returns a new plan with inserted materialization points
    */
  def insertMaterializationPoints(plan: DataflowPlan, globalOpGraph: GlobalOperatorGraph): DataflowPlan = {

    if(CliParams.values.profiling.isEmpty) {
      logger.info("profiling is disabled - won't try to find possible materialization points")
      return plan
    }

    logger.debug(s"analyzing plan for inserting materialization points")

    val ps = CliParams.values.profiling.get

    logger.debug(s"using prob threshold: ${ps.probThreshold}")
    logger.debug(s"using min benefit: ${ps.minBenefit}")

    var candidates = getCandidates(plan, globalOpGraph, ps)

    /* if unset, ps.minBenefit is actually Duration.Undefined
     * this, however, cannot be compared to Duration.Undefined, since this comparison is always false
     * But since Undefined is always greater than Inf, we use this fact for comparison
     */

    if(ps.minBenefit < Duration.Inf) {
      logger.debug(s"remove candidates with min benefit ${ps.minBenefit}")
      candidates = candidates.filter(p => p.benefit >= ps.minBenefit)
    }

    if(!java.lang.Double.isNaN(ps.probThreshold)) {
      logger.debug(s"remove candidates with min prob ${ps.probThreshold}")
      candidates = candidates.filter(p => p.prob >= ps.probThreshold)
    }

    logger.debug(s"still have ${candidates.size} materialization points - choose one with ${ps.strategy}")


    // just informative output for debugging/comparsion of different strategies
    // prints chosen points for each available strategy - independent from selected strategy
    GlobalStrategy.values.foreach{ s =>
      logger.debug(s"chosen points for $s \n${GlobalStrategy.getStrategy(s)(candidates,plan,globalOpGraph).mkString("\n")} ")
    }

    val chosenPoint = GlobalStrategy.getStrategy(ps.strategy)(candidates, plan, globalOpGraph)

    // for each candidate point ...
    // ... so that we will materialize the one with the greatest benefit
    var newPlan = plan
    chosenPoint.foreach{ m =>
      // ... determine the operator ...
      newPlan = materialize(m, newPlan)
    }

    newPlan.constructPlan(newPlan.operators)
    newPlan

  }


  private def materialize(m: MaterializationPoint, plan: DataflowPlan): DataflowPlan = {
    var newPlan = plan

    val lineage = m.lineage

    val theOp = newPlan.get(lineage) match {
      case Some(op) => op
      case None => throw OperatorNotFoundException(lineage)
    }

    logger.info(s"we chose to materialize ${theOp.name} ($lineage)")

    val ps = CliParams.values.profiling.get

    val path = generatePath(lineage)

    if(CliParams.values.compileOnly) {
      // if in compile only, we will not execute the script and thus not actually write the intermediate
      // results. Hence, we only create the path that would be used, but to not save the mapping
    } else {
      // ... that will be replaced with a Store op.
      saveMapping(lineage, path) // we save a mapping from the lineage of the actual op (not the materialize) to the path
    }

    val storer = Store(theOp.outputs.head, path.toString, Some(MaterializationManager.STORAGE_CLASS))
    if(ps.cacheMode == CacheMode.NONE) {
      newPlan.addOperator(Seq(storer), deferrConstruct = false)
      newPlan = newPlan.insertAfter(theOp, storer)

    } else {
      logger.debug(s" -> adding cache operator after ${theOp.name} (${theOp.outPipeNames.mkString(",")}) with cache-mode: ${ps.cacheMode}")

      val cache = Cache(Pipe(theOp.outPipeName), Pipe(PipeNameGenerator.generate(), producer= theOp), theOp.lineageSignature, ps.cacheMode)

      newPlan.addOperator(Seq(storer, cache), deferrConstruct = true)
      newPlan = newPlan.insertAfter(theOp, storer)
      cache.outputs = theOp.outputs
      newPlan = ProfilingSupport.insertBetweenAll(theOp.outputs.head, theOp, cache, newPlan)
    }

    newPlan
  }

  private def getCandidates(plan: DataflowPlan, globalOpGraph: GlobalOperatorGraph, ps: ProfilerSettings) = {
    // we add all potential points into a list first
    val candidates = mutable.Set.empty[MaterializationPoint]

    // traverse the plan and see if the current operator should be materialized
    DepthFirstTopDownWalker.walk(plan) {
      case _: TimingOp => // ignore timing ops

      case op if
      !candidates.contains(MaterializationPoint.dummy(op.lineageSignature)) && // only if not already analyzed
        op.outputs.nonEmpty && op.inputs.nonEmpty => // not sink and not source operator

        val sig = op.lineageSignature

        // try to get total costs up to this operator from the model
        globalOpGraph.totalCost(sig, ProbStrategy.func(ps.probStrategy))(CostStrategy.func(ps.costStrategy)) match {

          case Some((cost, prob)) =>
            val relProb = prob / globalOpGraph.totalRuns

            val outRecords = globalOpGraph.resultRecords(sig)
            val outputBPR = globalOpGraph.bytesPerRecord(sig)
            // total number of bytes
            val opOutputSize = outRecords.flatMap(r => outputBPR.map(_ * r))


            if(opOutputSize.isDefined) {
              val opSizeBytes = opOutputSize.get

              val opSizeMib = opSizeBytes / 1024 / 1024

              logger.debug(s"${op.name} (${op.outPipeNames.mkString(",")}|${op.lineageSignature})\t: " +
                            f"cost=${cost.milliseconds.toSeconds} \t prob=$relProb%2.2f\t" +
                            f"records =${outRecords.getOrElse("n/a")} r | ${outputBPR.getOrElse("n/a")} bytes/r = $opSizeMib%2.3f MiB")

              val writingTime = (opSizeMib / Conf.MiBPerSecWriting).seconds
              val readingTime = (opSizeMib / Conf.MiBPerSecReading).seconds

              logger.debug(f"\twriting $opSizeBytes bytes ($opSizeMib%2.2f MiB) would take ${writingTime.toSeconds} seconds")
              logger.debug(f"\treading $opSizeBytes bytes ($opSizeMib%2.2f MiB) would take ${readingTime.toSeconds} seconds")
              val benefit = cost.milliseconds - readingTime

              logger.info(s"\t--> benefit: ${benefit.toSeconds}")

              val m = MaterializationPoint(sig, cost = cost, prob = relProb, benefit = benefit)
              candidates += m
            } else {
              logger.debug(s"no size info for ${op.name} (${op.outPipeNames.mkString(",")}|$sig)")
            }

            case None =>
              logger.debug(s"no profiling info for ${op.name} ($sig)")
        }

      case _ =>
    }
    candidates
  }


  def loadIntermediateResults(plan: DataflowPlan): (DataflowPlan, Boolean) = {


    var loaded = false

    DepthFirstBottomUpWalker.walk(plan) { op =>

      val sig = op.lineageSignature
      val opName = op.name

      logger.debug(s"checking for existing materialized results for $opName ($sig)") //${op.name}

      getDataFor(sig) match {
        case Some(uri) =>
          logger.info(s"loading materialized results for ${op.name} $sig")

          val loader = Load(Pipe(op.outPipeName), uri.toString, op.constructSchema, Some(MaterializationManager.STORAGE_CLASS))
          logger.debug(s"replacing ${op.name} with $loader")

          // add the new Load op to the list of operators in the plan
          plan.addOperator(Seq(loader), deferrConstruct = true)

          // the consumers of op
          val opConsumers = op.outputs


          /* remove Op and all its predecessors from plan
              we cannot use the DataflowPlan#remove method here. Somehow it messes up the pipes so that
              we don't have correct schema
           */
          val opAncestors = BreadthFirstBottomUpWalker.collect(plan, Seq(op))
          plan.operators = plan.operators.filterNot(opAncestors.contains) // ++ Seq(loader)

          // just to be sure, clear the outputs of op
          op.outputs = List.empty

          // at this point, op and all its transitive predecessors should be removed from plan.operators


          /* for every consumer that reads from op, make it read from our new Load now

              Usually, an operator has only one output pipe, from which multiple consumers
              can read. However, SPLIT for example produces multiple outputs

              For ever old consumer we have to:
                - set it as consumer of Load
                - remove the old operator from producer and add the load as a new producer
           */
          // for each consumer (Pipe) of Op
          opConsumers.foreach { outPipe =>
            // for each consumer of that pipe
            outPipe.consumer.foreach { consumer =>

              // make this consumer a consumer of the new Load
              loader.addConsumer(outPipe.name, consumer)

              // and remove the old pipe and add Load as producer
              consumer.inputs = {
                // we need to replace the old pipe with the new pipe from the Loader
                // so first find the position of the old one
                val idx = consumer.inputs.indexWhere(_.name == outPipe.name)

                val (left,right) = consumer.inputs.splitAt(idx)
                // and then insert the new loader instead of the old op
                left ++ loader.outputs ++ right.drop(1) // drop 1 to remove the old pipe

                // CAUTION: THE FOLLOWING IS NOT CORRECT! the will reorder the pipes and then,
                // for a Join, we won't find the field in the schema!!
//                loader.outputs ++ consumer.inputs.filterNot(_.name == outPipe.name)
              }
            }
          }

          loaded = true

        case None => // nothing to do
      }

    }

    plan.constructPlan(plan.operators)
    (plan,loaded)
  }

  /**
   * Checks if we have materialized results for the given hash value
   * 
   * @param lineage The hash value to get data for
   * @return Returns the path to the materialized result, iff present. Otherwise <code>null</code>
   */
  def getDataFor(lineage: Lineage): Option[URI] = materializations.get(lineage)
    
  /**
   * Generate a path for the given lineage/hash value
   * 
   * @param lineage The identifier (lineage) of an operator
   * @return Returns the path where to store the result for this operator
   */
  def generatePath(lineage: Lineage): URI = matBaseDir.resolve(lineage)
  

  /**
   * Persist the given mapping of a hashcode to a specific file name.
   * 
   * @param lineage The hash code of the sub plan to persist
   * @param matFile The path to the file in which the results were materialized
   */
  def saveMapping(lineage: Lineage, matFile: URI) = {

    require(!CliParams.values.compileOnly, "writing materialization mapping info in compile-only mode will break things!")

    materializations += lineage -> matFile

    val json = write(materializations.map{case(k,v) => (k,v.toString)})

    Files.write(Conf.materializationMapFile,
      List(json).asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
        
  }
    
}
