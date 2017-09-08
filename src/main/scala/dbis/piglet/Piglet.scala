/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dbis.piglet

import java.nio.file.{Path, Paths}

import dbis.piglet.backends.BackendManager
import dbis.piglet.codegen.PigletCompiler
import dbis.piglet.mm.{DataflowProfiler, MaterializationManager, ProfilingException, StatServer}
import dbis.piglet.plan.rewriting.Rewriter._
import dbis.piglet.plan.rewriting.Rules
import dbis.piglet.plan.{DataflowPlan, InvalidPlanException, PlanMerger}
import dbis.piglet.schema.SchemaException
import dbis.piglet.tools._
import dbis.piglet.tools.logging.PigletLogging
import dbis.setm.SETM
import dbis.setm.SETM.timing

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._


object Piglet extends PigletLogging {

  type Lineage = String

  def main(args: Array[String]): Unit = {

    // Parse CLI parameters
    CliParams.parse(args)

    // start statistics collector SETM if needed
    startCollectStats(CliParams.values.showStats, CliParams.values.quiet)
    

    // set the log level as defined in the parameters
		logger.setLevel(CliParams.values.logLevel)

    CliParams.values.profiling.foreach(ps => logger.debug(ps.toString))


    /* Copy config file to the user's home directory
     * IMPORTANT: This must be the first call to Conf
     * Otherwise, the config file was already loaded before we could copy the new one
     */
    if (CliParams.values.updateConfig) {
      // in case of --update we just copy the config file and exit
      Conf.copyConfigFile()
      println(s"Config file copied to ${Conf.programHome} - exitting now")
      sys.exit()
    }

    // set default backend if necessary now - we had to "wait" until after the update conf call
//    val tb = c.backend.getOrElse(Conf.defaultBackend)
    

    // get the input files
    val files = CliParams.values.inputFiles.takeWhile { p => !p.getFileName.startsWith("-") }
    if (files.isEmpty) {
      // because the file argument was optional we have to check it here
      println("Error: Missing argument <file>...\nTry --help for more information.")
      sys.exit(-1)
    }

    

    /* add the parameters supplied via CLI to the paramMap after we read the file
     * this way, we can override the values in the file via CLI
     */

    if(CliParams.values.params.nonEmpty)
    	logger.debug(s"provided parameters: ${CliParams.values.params.map{ case (k,v) => s"$k -> $v"}.mkString("\n")}")


    //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    run()  // this little call starts the whole processing!
    //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
    
    // at the end, show the statistics
    if(CliParams.values.showStats) {
      // collect and print runtime stats
      collectStats()
    }

  } // main


  /**
    * Start compiling the Pig script into a the desired program
    */
  def run(): Unit = {
    var success = true
    try {

      // initialize backend
      BackendManager.init(CliParams.values.backend)

      if (BackendManager.backend.raw) {
        if (CliParams.values.compileOnly) {
          logger.error("Raw backends do not support compile-only mode! Aborting")
          return
        }

        if (CliParams.values.profiling.isDefined) {
          logger.error("Raw backends do not support profiling yet! Aborting")
          return
        }

        // process each file separately - one after the other
        CliParams.values.inputFiles.foreach { file => runRaw(file, CliParams.values.master, CliParams.values.backendArgs) }

      } else {
        // no raw backend, generate code and submit job
        runWithCodeGeneration()
      }

    } catch {
      // don't print full stack trace to error
      case e: Exception =>
        logger.error(s"An error occured: ${e.getMessage}")
        logger.debug("Stackstrace: ", e)
        success = false
    } finally {
      if(CliParams.values.notifyURL.isDefined) {
        
        val stringURI = CliParams.values.notifyURL.get.toString
          .replace("[success]", if(success) "Success" else "Failed")
          .replace("[name]", CliParams.values.inputFiles.map(_.getFileName.toString()).mkString(","))
          .replace("[time]", java.time.LocalDateTime.now().toString)

        logger.debug(s"notification url: $stringURI")  
          
        val result = scalaj.http.Http(stringURI).asString
        logger.debug(s"notification HTTP service responeded with: ${result.body}")
      } else {
        logger.debug("finished.")
      }
    }
  }

  /**
    *
    * @param file The pig script file to execute
    * @param master The master info
    * @param backendArgs Additional arguments to the target runtime
    */
  def runRaw(file: Path, master: String, backendArgs: Map[String, String]) = timing("execute raw") {
    logger.debug(s"executing in raw mode: $file with master $master for backend ${BackendManager.backend.name} with arguments ${backendArgs.mkString(" ")}")
    val runner = BackendManager.backend.runnerClass
    runner.executeRaw(file, master, backendArgs)
  }


  /**
   * Run with the provided set of files using the specified backend, that is _not_ a raw backend.
   *
   */
  def runWithCodeGeneration(): Unit = timing("run with generation") {


    logger.debug("start parsing input files")

    var schedule = ListBuffer.empty[(DataflowPlan,Path)]

    for(file <- CliParams.values.inputFiles) {
      // foreach file, generate the data flow plan and store it in our schedule
      PigletCompiler.createDataflowPlan(file, CliParams.values.params, CliParams.values.backend) match {
        case Some(v) => schedule += ((v, file))
        case None => // in case of an error (no plan genrated for file) abort current execution
          throw InvalidPlanException(s"failed to create dataflow plan for $file - aborting")
      }
    }


    /*
     * if we have got more than one plan and we should not execute them
     * sequentially, then try to merge them into one plan
     */
    if(schedule.size > 1 && !CliParams.values.sequential) {
      logger.debug("Start merging plans")

      // merge plans into one plan
      val mergedPlan = PlanMerger.mergePlans( schedule.map{case (plan, _) => plan } )

      // adjust the new schedule. It now contains only the merged plan, with a new generated file name
      schedule = ListBuffer((mergedPlan, Paths.get(s"merged_${System.currentTimeMillis()}.pig")))
    }


		// begin global analysis phase
    DataflowProfiler.load(Conf.programHome)

    logger.debug("start processing created dataflow plans")

    for ((plan, path) <- schedule) timing("execute plan") {
      
      logger.info(s"processing plan for $path")
      //TODO:this is ugly in this place here... maybe we should create a "clear-wrapper"
      dbis.piglet.codegen.scala_lang.JoinEmitter.joinKeyVars.clear()

      
      var newPlan = plan

      val mm = new MaterializationManager(Conf.materializationBaseDir)

      // 1. process EXPLICIT Materialize operators
      // insert STORE or LOAD operators
      newPlan = processMaterializations(newPlan, mm)

      // 2. rewrite WINDOW operators for Flink streaming
      if (CliParams.values.backend == "flinks")
        newPlan = processWindows(newPlan)

      // 3. apply general optimization rules
      Rules.registerBackendRules(CliParams.values.backend)
      newPlan = rewritePlan(newPlan)

      // 4. check if we already have materialized results
      // and insert LOAD operators
      newPlan = mm.loadIntermediateResults(newPlan)


      // 5. analyze plan if something can be materialized or
      // if materialized results are present
      if(CliParams.values.profiling.isDefined) {
        val model = DataflowProfiler.analyze(newPlan)

        // according to statistics, insert MATERIALIZE (STORE) operators
        newPlan = mm.insertMaterializationPoints(newPlan, model)
      }
//      newPlan = processMaterializations(newPlan, mm)


      // 6. after all optimizations have been performed, insert
      // profiling operators (if desired)
      if(CliParams.values.profiling.isDefined) {
        // after rewriting the plan, add the timing operations
        newPlan = insertTimings(newPlan)
      }

      // for testing of scripts and Piglet features, consumers
      // such as Dump and Store may be muted
      if(CliParams.values.muteConsumer) {
        newPlan = mute(newPlan)
      }

      logger.debug("finished optimizations")

      val scriptName = path.getFileName.toString.replace(".pig", "")
      logger.debug(s"using script name: $scriptName")

      // for creating the Dot image
      PlanWriter.init(newPlan)
      val imgPath = path.resolveSibling(scriptName)
      PlanWriter.createImage(imgPath, scriptName)

      if (CliParams.values.showPlan) {
        println("final plan = {")
        newPlan.printPlan(2)
        println("}")
      }

      // 7. check if the plan is still valid
      try {

        newPlan.checkConsistency
        // if this does _not_ throw an exception, the schema is ok
        newPlan.checkSchemaConformance()

      } catch {
        case InvalidPlanException(msg) =>
          logger.error(s"inconsistent plan in $msg")
          return
        case SchemaException(msg) =>
          logger.error(s"schema conformance error in $msg")
          return
      }




      // 8. compile the plan to code
      PigletCompiler.compilePlan(newPlan, scriptName) match {
        // the file was created --> execute it
        case Some(jarFile) =>
          if (!CliParams.values.compileOnly) {

            if (CliParams.values.profiling.isDefined) {
              logger.debug("starting stat server")
              StatServer.start(CliParams.values.profiling.get)
            }

            // 9. and finally deploy/submit
            val runner = BackendManager.backend.runnerClass
            logger.debug(s"using runner class ${runner.getClass.toString}")

            logger.info( s"""starting job at "$jarFile" using backend "${CliParams.values.backend}" """)

            timing("job execution") {
              val start = System.currentTimeMillis()
              runner.execute(CliParams.values.master, scriptName, jarFile, CliParams.values.backendArgs, CliParams.values.profiling.isDefined)

              logger.info(s"program execution finished in ${System.currentTimeMillis() - start} ms")

            }


          } else
            logger.info("successfully compiled program - exiting.")


          // after execution we want to write the dot file
          if (CliParams.values.profiling.isDefined && !CliParams.values.compileOnly) {

            try {
              DataflowProfiler.collect()

              /* if we really have a batch of scripts, then writing the statistics after
               * each plan is a performance issue. However, doing it at this place allows
               * us to easily react on errors: We don't want to write the statistics if
               * there was an error...
               */
              DataflowProfiler.writeStatistics()

            } catch {
              case ProfilingException(msg) =>
                logger.error(s"error in profiler: $msg")
            }

            newPlan.operators.foreach { node =>
              val time = DataflowProfiler.getExectime(node.lineageSignature).map(t => t.avg().milliseconds)

              PlanWriter.nodes(node.lineageSignature).time = time
            }

            val scLineage = "sparkcontext"
            PlanWriter.nodes += scLineage -> Node(scLineage, DataflowProfiler.getExectime(scLineage).map(_.max.milliseconds), PlanWriter.quote("Spark Context"))
            newPlan.sourceNodes.map(PlanWriter.signature).foreach { load => PlanWriter.edges += Edge(scLineage, load) }

          }

          //          PlanWriter.writeDotFile(jarFile.getParent.resolve(s"$scriptName.dot"))
          PlanWriter.createImage(jarFile.getParent, scriptName)
        case None => logger.error(s"creating jar file failed for $path")
      }
    }


    // if the StatServer was created, stop it now
    if(CliParams.values.profiling.isDefined)
      StatServer.stop()

  }

  def startCollectStats(enable: Boolean, quiet: Boolean) = {
    if(enable) 
      SETM.enable 
    else 
      SETM.disable
      
    SETM.quiet = quiet
  }
  
  def collectStats() = SETM.collect()

}
