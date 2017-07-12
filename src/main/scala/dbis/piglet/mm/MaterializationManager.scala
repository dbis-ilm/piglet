package dbis.piglet.mm
import java.net.URI
import java.nio.file.{Files, StandardOpenOption}

import dbis.piglet.Piglet.Lineage
import dbis.piglet.op._
import dbis.piglet.plan.rewriting.internals.ProfilingSupport
import dbis.piglet.plan.{DataflowPlan, OperatorNotFoundException}
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

  def replaceWithLoad(materialize: PigOperator, path: URI, plan: DataflowPlan): DataflowPlan = {

    val loader = Load(materialize.inputs.head, path, materialize.constructSchema, Some("BinStorage"))
    val matInput = materialize.inputs.head.producer

    var newPlan = plan

    for (inPipe <- matInput.inputs) {
      newPlan.disconnect(inPipe.producer, matInput)
    }

    newPlan = newPlan.replace(matInput, loader)

    logger.info(s"replaced materialize op with loader $loader")

    /* TODO: do we need to remove all other nodes that get disconnected now by hand
       * or do they get removed during code generation (because there is no sink?)
       */
    newPlan = newPlan.remove(materialize)

    newPlan
  }

  def replaceWithStore(materialize: Materialize, path: URI, plan: DataflowPlan): DataflowPlan = {

    var newPlan = plan


    val storer = Store(materialize.inputs.head, path, Some("BinStorage"))

    newPlan = newPlan.insertAfter(materialize.inputs.head.producer, storer)
    newPlan = newPlan.remove(materialize)

    logger.info(s"inserted new store operator $storer")

    newPlan
  }
}


/**
  * Manage where materialized intermediate results are stored
  */
class MaterializationManager(private val matBaseDir: URI, c: CliParams) extends PigletLogging {

  implicit val formats = Serialization.formats(NoTypeHints)

  logger.debug(s"materialization base directory: $matBaseDir")
  //  logger.debug(s"using materialization storage service at $url")

  require(matBaseDir != null, "Base directory for materialization must not be null")

  var materializations: Map[Lineage, URI] = if (Files.exists(Conf.materializationMapFile)) {
    val json = Source.fromFile(Conf.materializationMapFile.toFile).getLines().mkString("\n")

    if(json.isEmpty)
      Map.empty[Lineage, URI]
    else
      parse(json).extract[Map[Lineage, String]].map{case(k,v) => (k,new URI(v))}

  } else {
    Map.empty[Lineage, URI]
  }

  def insertMaterializationPoints(plan: DataflowPlan, model: Markov): DataflowPlan = {



    if(c.profiling.isEmpty) {
      logger.info("profiling is disabled - won't try to find possible materialization points")
      return plan
    }

    logger.debug(s"analyzing plan for inserting materialization points")

    val ps = c.profiling.get

    logger.debug(s"using prob threshold: ${ps.probThreshold}")
    logger.debug(s"using min benefit: ${ps.minBenefit}")

    val bytesPerSecWriting = 47.93590545654297 * 1024 * 1024 // Bytes/sec

    val candidates = mutable.ListBuffer.empty[MaterializationPoint]

    DepthFirstTopDownWalker.walk(plan) {
      case _: TimingOp =>
      case op if !candidates.contains(MaterializationPoint(op.lineageSignature,Duration.Undefined,-1,-1)) =>
        val sig = op.lineageSignature
        model.totalCost(sig, ProbStrategy.func(ps.probStrategy))(CostStrategy.func(ps.costStrategy)) match {
          case Some((cost,prob)) =>
            val relProb = prob // / model.totalRuns
            val opOutputSize = model.outputSize(sig)

            if(opOutputSize.isDefined) {

              /*
               * FIXME: here we compare time (cost) with num records (outputSize)
               */

              val benefit = (cost - opOutputSize.get / bytesPerSecWriting).seconds
              val decision = benefit > ps.minBenefit && relProb > ps.probThreshold

              if(decision) {
                logger.debug(s"We should materialize ${op.name} with benefit of ${benefit.toSeconds}")
                candidates += MaterializationPoint(sig, benefit, relProb, cost)
              } else
                logger.debug(s"We should NOT materialize ${op.name} with benefit of ${benefit.toSeconds}")

            }

            logger.debug(s"${op.name} (${op.lineageSignature})\t: " +
              s"cost=$cost \t prob=$relProb\t" +
              s"inputsize=${model.inputSize(sig).getOrElse("")}\t" +
              s"outputsize=${opOutputSize.getOrElse("")}")

          case None =>
        }
    }




    // here we might have a long list of possible MaterializationPoint s - we should only select the most important ones
    // On the other hand, we already decided, that all these candidates are important!


    var newPlan = plan

    // for each candidate point ...
    candidates.sortBy(_.benefit)(Ordering[Duration].reverse) // descending ordering ...
      .headOption                                            // ... so that we will materialize the one with the greatest benefit
      .foreach{ case MaterializationPoint(lineage, _,_,_) =>

        // ... determine the operator ...
        val theOp = newPlan.get(lineage) match {
          case Some(op) => op
          case None => throw OperatorNotFoundException(lineage)
        }

        val path = if(c.compileOnly) {
          // if in compile only, we will not execute the script and thus not actually write the intermediate
          // results. Hence, we only create the path that would be used, but to not save the mapping
          generatePath(lineage)
        } else {
          // ... that will be replaced with a Store op.
          saveMapping(lineage) // we save a mapping from the lineage of the actual op (not the materialize) to the path
        }

        val storer = Store(theOp.outputs.head, path, Some("BinStorage"))
        if(ps.cacheMode == CacheMode.NONE) {
          newPlan.addOperator(Seq(storer), deferrConstruct = false)
          newPlan = newPlan.insertAfter(theOp, storer)

        } else {
          logger.debug(s"adding cache operator afters $theOp  with cache-mode: ${ps.cacheMode}")
          val cache = Cache(Pipe("dummy"), Pipe("dummy", producer= theOp), theOp.lineageSignature, ps.cacheMode)

          newPlan.addOperator(Seq(storer, cache), deferrConstruct = true)
          newPlan = newPlan.insertAfter(theOp, storer)
          newPlan = ProfilingSupport.insertBetweenAll(theOp.outputs.head, theOp, cache, newPlan)
        }
    }

    newPlan

  }


  def loadIntermediateResults(plan: DataflowPlan): DataflowPlan = {
    var newPlan = plan

    DepthFirstTopDownWalker.walk(newPlan) { op =>

      val sig = op.lineageSignature

      logger.debug(s"checking for existing materialized results for ${op.name} ($sig)")

      getDataFor(sig) match {
        case Some(uri) =>
          logger.info(s"loading materialized results for ${op.name} $sig")

          val loader = Load(op.outputs.head, uri, op.schema, Some("BinStorage"))
          logger.debug(s"replacing ${op.name} with $loader")

          // add the new Load op to the list of operators in the plan
          newPlan.addOperator(Seq(loader), deferrConstruct = true)

          // the consumers of op
          val opConsumers = op.outputs

          // remove Op and all its predecessors from plan
          newPlan = newPlan.remove(op, removePredecessors = true)

          // for each consumer (Pipe) of Op
          opConsumers.foreach { s =>
            // for each consumer of that pipe
            s.consumer.foreach { c =>
              // make this consumer a consumer of the new Load
              loader.addConsumer(s.name, c)
              // and remove the old pipe and add Load as producer
              c.inputs = c.inputs.filter(_.name == s.name) ++ loader.outputs

              // update the schema of the consumers to adapt to new input op
              // actually the schema should be the same, but somehow types were not correctly inferred in consumers
              c.constructSchema
            }
          }

        case None => // nothing to do
      }

    }

    newPlan
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
  private def generatePath(lineage: Lineage): URI = matBaseDir.resolve(lineage)
  
  /**
    * Saves a mapping of the lineage of an operator to its materialization location.
    * @param lineage The lineage of the operator to save
    * @return Returns the Path to materialized results
    */
  def saveMapping(lineage: Lineage): URI = {
    val matFile = generatePath(lineage)
    saveMapping(lineage, matFile)
    matFile
  }
  
  
  /**
   * Persist the given mapping of a hashcode to a specific file name.
   * 
   * @param lineage The hash code of the sub plan to persist
   * @param matFile The path to the file in which the results were materialized
   */
  private def saveMapping(lineage: Lineage, matFile: URI) = {
    materializations += lineage -> matFile

    val json = write(materializations.map{case(k,v) => (k,v.toString)})



    Files.write(Conf.materializationMapFile,
      List(json).asJava,
      StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
        
  }
    
}
