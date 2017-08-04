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

    /* This was measured using HDFSIO tools to benchmark HDFS performance for our cluster
        The value was given in MB/sec but since we collect the statistics in Bytes/sec
        we have to convert its
     */


    val candidates = mutable.Set.empty[MaterializationPoint]

    DepthFirstTopDownWalker.walk(plan) {
      case _: TimingOp =>
      case op if
        !candidates.contains(MaterializationPoint.dummy(op.lineageSignature)) && // only if not already analyzed
          op.outputs.nonEmpty => // not sink operator
        val sig = op.lineageSignature

        model.totalCost(sig, ProbStrategy.func(ps.probStrategy))(CostStrategy.func(ps.costStrategy)) match {
          case Some((cost,prob)) =>
            val relProb = prob // / model.totalRuns

            val outRecords = model.resultRecords(sig).map(_ * ps.fraction)
            val outputBPR = model.bytesPerRecord(sig)

            // total number of bytes
            val opOutputSize = outRecords.flatMap(r => outputBPR.map(_ * r))

            if(opOutputSize.isDefined) {
              logger.debug(s"${op.name} (${op.lineageSignature})\t: " +
                s"cost=${cost.milliseconds.toSeconds} \t prob=$relProb\t" +
                s"sampled records =${model.resultRecords(sig).getOrElse("n/a")} r | ${model.bytesPerRecord(sig).getOrElse("n/a")} bytes/r\t" +
                s"real records=${outRecords.getOrElse("n/a")} r")


              val writingTime = (opOutputSize.get / Conf.BytesPerSecWriting).seconds
              val readingTime = (opOutputSize.get / Conf.BytesPerSecReading).seconds

              logger.debug(s"\twriting for ${op.name} ($sig) with ${opOutputSize.get} bytes would take ${writingTime.toSeconds} seconds")
              logger.debug(s"\treading for ${op.name} ($sig) with ${opOutputSize.get} bytes would take ${readingTime.toSeconds} seconds")

              val benefit = cost.milliseconds - readingTime

              val costDecision = readingTime < cost.milliseconds - ps.minBenefit

              val decision = costDecision && relProb > ps.probThreshold

              if(decision) {
                logger.debug(s"\t--> We should materialize ${op.name} with benefit of ${benefit.toSeconds} and prob = $relProb")
                candidates += MaterializationPoint(sig, benefit, relProb, cost)
              } else
              logger.debug(s"\t--> We should NOT materialize ${op.name} with benefit of ${benefit.toSeconds} and prob = $relProb")

            }


          case None =>
            logger.debug(s"no profiling info for ${op.name} ($sig)")
        }
      case op => logger.debug(s"irgendwas mit dummy for $op")
    }




    // here we might have a long list of possible MaterializationPoint s - we should only select the most important ones
    // On the other hand, we already decided, that all these candidates are important!


    var newPlan = plan

    // for each candidate point ...
    candidates.toSeq.sortBy(_.benefit)(Ordering[Duration].reverse) // descending ordering ...
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

          val cache = Cache(Pipe(theOp.outPipeName), Pipe(PipeNameGenerator.generate(), producer= theOp), theOp.lineageSignature, ps.cacheMode)

          newPlan.addOperator(Seq(storer, cache), deferrConstruct = true)
          newPlan = newPlan.insertAfter(theOp, storer)
          cache.outputs = theOp.outputs
          newPlan = ProfilingSupport.insertBetweenAll(theOp.outputs.head, theOp, cache, newPlan)
        }
    }

    newPlan

  }


  def loadIntermediateResults(plan: DataflowPlan): DataflowPlan = {
//    val newPlan = plan

    DepthFirstTopDownWalker.walk(plan) { op =>

      val sig = op.lineageSignature

      logger.debug(s"checking for existing materialized results for ${op.name} ($sig)")

      getDataFor(sig) match {
        case Some(uri) =>
          logger.info(s"loading materialized results for ${op.name} $sig")

//          val loader = Load(op.outputs.head, uri, op.schema, Some("BinStorage"))
          //FIXME: could be a problem when restoring the result of a SPLIT operator. Then we would have to insert multiple LOADs?

//          val newSchema = op.schema.map{ oldSchema =>
//            val newFields = oldSchema.fields.map{ field => Field(field.name, field.fType) }
//
//            Schema(newFields)
//
//          }

          val loader = Load(Pipe(op.outPipeName), uri, op.schema, Some("BinStorage"))
//          loader.outputs.head.producer = loader
          logger.debug(s"replacing ${op.name} with $loader")

          // add the new Load op to the list of operators in the plan
          plan.addOperator(Seq(loader), deferrConstruct = true)

          // the consumers of op
          val opConsumers = op.outputs


          /* remove Op and all its predecessors from plan
              we cannot use the DataflowPlan#remove method here. Somehow it messes up the pipes so that
              we don't have correct schema
           */
//          newPlan = newPlan.remove(op, removePredecessors = true)
          val opAncestors = BreadthFirstBottomUpWalker.collect(plan, Seq(op))
          plan.operators = plan.operators.filterNot(opAncestors.contains) // ++ Seq(loader)

          // just to be sure, clear the outputs of op
          op.outputs = List.empty

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
              consumer.inputs = loader.outputs ++ consumer.inputs.filterNot(_.name == outPipe.name)
            }
          }

        case None => // nothing to do
      }

    }

    plan.constructPlan(plan.operators)
    plan
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
