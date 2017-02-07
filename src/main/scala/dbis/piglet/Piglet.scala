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

import scala.io.Source
import scala.collection.mutable.ListBuffer

import scopt.OptionParser

import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.Files
import java.net.URI

import dbis.piglet.backends.BackendManager
import dbis.piglet.backends.BackendConf
import dbis.piglet.codegen.PigletCompiler
import dbis.piglet.mm.{DataflowProfiler, MaterializationPoint}
import dbis.piglet.op.PigOperator
import dbis.piglet.parser.PigParser
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.plan.InvalidPlanException
import dbis.piglet.plan.MaterializationManager
import dbis.piglet.plan.PlanMerger
import dbis.piglet.plan.rewriting.Rewriter._
import dbis.piglet.plan.rewriting.Rules
import dbis.piglet.schema.SchemaException
import dbis.piglet.tools.FileTools
import dbis.piglet.tools.Conf
import dbis.piglet.tools.{DepthFirstTopDownWalker, BreadthFirstTopDownWalker}
import dbis.piglet.tools.logging.PigletLogging
import dbis.piglet.tools.logging.LogLevel
import dbis.piglet.tools.logging.LogLevel._
import dbis.piglet.tools.CliParams

import dbis.setm.SETM
import dbis.setm.SETM.timing
import java.util.Formatter.DateTime
import dbis.piglet.mm.StatServer
import dbis.piglet.tools.PlanWriter
import dbis.piglet.tools.TopoSort


object Piglet extends PigletLogging {


  def main(args: Array[String]): Unit = {

    // Parse CLI parameters
    val c = CliParams.parse(args)

    
    // start statistics collector SETM if needed
    startCollectStats(c.showStats, c.quiet)
    

    // set the log level as defined in the parameters
		logger.setLevel(c.logLevel)


    /* Copy config file to the user's home directory
     * IMPORTANT: This must be the first call to Conf
     * Otherwise, the config file was already loaded before we could copy the new one
     */
    if (c.updateConfig) {
      // in case of --update we just copy the config file and exit
      Conf.copyConfigFile()
      println(s"Config file copied to ${Conf.programHome} - exitting now")
      sys.exit()
    }

    // set default backend if necessary now - we had to "wait" until after the update conf call
//    val tb = c.backend.getOrElse(Conf.defaultBackend)
    

    // get the input files
    val files = c.inputFiles.takeWhile { p => !p.getFileName.startsWith("-") }
    if (files.isEmpty) {
      // because the file argument was optional we have to check it here
      println("Error: Missing argument <file>...\nTry --help for more information.")
      sys.exit(-1)
    }

    

    /* add the parameters supplied via CLI to the paramMap after we read the file
     * this way, we can override the values in the file via CLI
     */

    if(c.params.nonEmpty)
    	logger.debug(s"provided parameters: ${c.params.map{ case (k,v) => s"$k -> $v"}.mkString("\n")}")


    //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    run(c)  // this little call starts the whole processing!
    //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
    
    if(c.profiling.isDefined) {
    	StatServer.stop()

    	DataflowProfiler.writeStatistics(c)
    }
    
    // at the end, show the statistics
    if(c.showStats) {
      // collect and print runtime stats
      collectStats
    }

  } // main


  /**
    * Start compiling the Pig script into a the desired program
    */
  def run(c: CliParams): Unit = {
    var success = true
    try {

      // initialize backend
      BackendManager.init(c.backend)

      if (BackendManager.backend.raw) {
        if (c.compileOnly) {
          logger.error("Raw backends do not support compile-only mode! Aborting")
          return
        }

        if (c.profiling.isDefined) {
          logger.error("Raw backends do not support profiling yet! Aborting")
          return
        }

        // process each file separately - one after the other
        c.inputFiles.foreach { file => runRaw(file, c.master, c.backendArgs) }

      } else {
        // no raw backend, generate code and submit job
        runWithCodeGeneration(c)
      }

    } catch {
      // don't print full stack trace to error
      case e: Exception =>
        logger.error(s"An error occured: ${e.getMessage}")
        logger.debug("Stackstrace: ", e)
        success = false
    } finally {
      if(c.notifyURL.isDefined) {
        
        val stringURI = c.notifyURL.get.toString()
          .replace("[success]", if(success) "Success" else "Failed")
          .replace("[name]", c.inputFiles.map(_.getFileName.toString()).mkString(","))
          .replace("[time]", java.time.LocalDateTime.now().toString())

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
    * @param file
    * @param master
    * @param backendConf
    * @param backendArgs
    */
  def runRaw(file: Path, master: String, backendArgs: Map[String, String]) = timing("execute raw") {
    logger.debug(s"executing in raw mode: $file with master $master for backend ${BackendManager.backend.name} with arguments ${backendArgs.mkString(" ")}")
    val runner = BackendManager.backend.runnerClass
    runner.executeRaw(file, master, backendArgs)
  }


  /**
   * Run with the provided set of files using the specified backend, that is _not_ a raw backend.
   *
   * @param c Parameter settings / configuration
   * 
   */
  def runWithCodeGeneration(c: CliParams): Unit = timing("run with generation") {


    logger.debug("start parsing input files")

    var schedule = ListBuffer.empty[(DataflowPlan,Path)]

    for(file <- c.inputFiles) {
      // foreach file, generate the data flow plan and store it in our schedule
      PigletCompiler.createDataflowPlan(file, c.params, c.backend) match {
        case Some(v) => schedule += ((v, file))
        case None => // in case of an error (no plan genrated for file) abort current execution
          throw InvalidPlanException(s"failed to create dataflow plan for $file - aborting")
      }
    }


    /*
     * if we have got more than one plan and we should not execute them
     * sequentially, then try to merge them into one plan
     */
    if(schedule.size > 1 && !c.sequential) {
      logger.debug("Start merging plans")

      // merge plans into one plan
      val mergedPlan = PlanMerger.mergePlans( schedule.map{case (plan, _) => plan } )

      // adjust the new schedule. It now contains only the merged plan, with a new generated file name
      schedule = ListBuffer((mergedPlan, Paths.get(s"merged_${System.currentTimeMillis()}.pig")))
    }


		// begin global analysis phase

		// count occurrences of each operator in schedule
    if(c.profiling.isDefined) {
      val file = c.profiling.get.resolve(Conf.opCountFile)
      DataflowProfiler.createOpCounter(schedule, c)

      StatServer.start(Conf.statServerPort)
    }

    logger.debug("start processing created dataflow plans")

    for ((plan, path) <- schedule) timing("execute plan") {
      
      logger.info(s"processing plan for $path")
      //TODO:this is ugly in this place here... maybe we should create a "clear-wrapper"
      dbis.piglet.codegen.scala_lang.JoinEmitter.joinKeyVars.clear()

      
      // 3. now, we should apply optimizations
      var newPlan = plan

      // process explicit MATERIALIZE operators
      if (c.profiling.isDefined) {
        val mm = new MaterializationManager(Conf.materializationBaseDir)
        newPlan = processMaterializations(newPlan, mm)
      }


      // rewrite WINDOW operators for Flink streaming
      if (c.backend == "flinks")
        newPlan = processWindows(newPlan)

      Rules.registerBackendRules(c.backend)
      newPlan = rewritePlan(newPlan)

      
      // after rewriting the plan, add the timing operations
      if(c.profiling.isDefined) {
        newPlan = insertTimings(newPlan)
      }

      if(c.muteConsumer) {
        newPlan = mute(newPlan)
      }
      
      
      // find materialization points
//      profiler.foreach { p => p.addMaterializationPoints(newPlan) }
      if(c.profiling.isDefined) {
    	  DataflowProfiler.init(c.profiling.get, newPlan)
        DataflowProfiler.addMaterializationPoints(newPlan)
      }

      logger.debug("finished optimizations")

      if (c.showPlan) {
        PlanWriter.init(newPlan)
        println("final plan = {")
        newPlan.printPlan(2)
        println("}")
      }

      try {
    	  newPlan.checkConsistency
      } catch {
        case e: InvalidPlanException => {
      	  logger.error(s"inconsistent plan in ${e.getMessage}")
      	  return
        }
      }
      try {
        // if this does _not_ throw an exception, the schema is ok
        newPlan.checkSchemaConformance
      } catch {
        case e: SchemaException => {
          logger.error(s"schema conformance error in ${e.getMessage}")
          return
        }
      }
      

      val scriptName = path.getFileName.toString().replace(".pig", "")
      logger.debug(s"using script name: $scriptName")


      PigletCompiler.compilePlan(newPlan, scriptName, c) match {
        // the file was created --> execute it
        case Some(jarFile) =>
          if (!c.compileOnly) {
            // 4. and finally deploy/submit
            val runner = BackendManager.backend.runnerClass
            logger.debug(s"using runner class ${runner.getClass.toString()}")

            logger.info( s"""starting job at "$jarFile" using backend "${c.backend}" """)

            timing("job execution") {
              runner.execute(c.master, scriptName, jarFile, c.backendArgs, c.profiling.isDefined)
            }
          } else
            logger.info("successfully compiled program - exiting.")
            
            
          // after execution we want to write the dot file  
          if(c.showPlan) {
            
            if(c.profiling.isDefined) {
              
              val exectimesCnt = DataflowProfiler.collect
              
              logger.info(s"profiler has info for ${exectimesCnt} lineages")
              newPlan.operators.foreach{ node => 
                val time = DataflowProfiler.getExectime(node.lineageSignature)
                
                PlanWriter.nodes(node.lineageSignature).time = time
              }
              
            }
            
            PlanWriter.writeDotFile(jarFile.getParent.resolve(s"${scriptName}.dot"))
          }

        case None => logger.error(s"creating jar file failed for ${path}")
      }
    }
  }

  def startCollectStats(enable: Boolean, quiet: Boolean) = {
    if(enable) 
      SETM.enable 
    else 
      SETM.disable
      
    SETM.quiet = quiet
  }
  
  def collectStats = SETM.collect()

}
