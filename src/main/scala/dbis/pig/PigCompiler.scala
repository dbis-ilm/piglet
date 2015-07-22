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

package dbis.pig


import java.io.File
import dbis.pig.op.PigOperator
import dbis.pig.parser.PigParser
import dbis.pig.plan.DataflowPlan
import dbis.pig.plan.rewriting.Rewriter._
import dbis.pig.schema.SchemaException
import dbis.pig.tools.FileTools
import scopt.OptionParser
import scala.io.Source
import dbis.pig.plan.MaterializationManager
import dbis.pig.tools.Conf
import com.typesafe.scalalogging.LazyLogging
import dbis.pig.backends.BackendManager

object PigCompiler extends PigParser with LazyLogging {
  
  case class CompilerConfig(master: String = "local",
                            input: String = "",
                            compile: Boolean = false,
                            outDir: String = ".",
                            params: Map[String,String] = Map(),
                            backend: String = Conf.defaultBackend) // XXX: does this work?

  def main(args: Array[String]): Unit = {
    var master: String = "local"
    var inputFile: String = null
    var compileOnly: Boolean = false
    var outDir: String = null
    var params: Map[String,String] = null
    var backend: String = null

    val parser = new OptionParser[CompilerConfig]("PigCompiler") {
      head("PigCompiler", "0.2")
      opt[String]('m', "master") optional() action { (x, c) => c.copy(master = x) } text ("spark://host:port, mesos://host:port, yarn, or local.")
      opt[Unit]('c', "compile") action { (_, c) => c.copy(compile = true) } text ("compile only (don't execute the script)")
      opt[String]('o',"outdir") optional() action { (x, c) => c.copy(outDir = x)} text ("output directory for generated code")
      opt[String]('b',"backend") optional() action { (x,c) => c.copy(backend = x)} text ("Target backend (spark, flink, ...)")
      opt[Map[String,String]]('p', "params") valueName("name1=value1,name2=value2...") action { (x, c) => c.copy(params = x) } text("parameter(s) to subsitute")
      help("help") text ("prints this usage text")
      version("version") text ("prints this version info")
      arg[String]("<file>") required() action { (x, c) => c.copy(input = x) } text ("Pig file")
    }
    // parser.parse returns Option[C]
    parser.parse(args, CompilerConfig()) match {
      case Some(config) => {
        // do stuff
        master = config.master
        inputFile = config.input
        compileOnly = config.compile
        outDir = config.outDir
        params = config.params
        backend = config.backend
      }
      case None =>
        // arguments are bad, error message will have been displayed
        return
    }
    
    // start processing
    run(inputFile, outDir, compileOnly, master, backend, params)
  }

  /**
   * Start compiling the Pig script into a the desired program
   */
  def run(inputFile: String, outDir: String, compileOnly: Boolean, master: String, backend: String, params: Map[String,String]): Unit = {
    
    // 1. we read the Pig file
    val source = Source.fromFile(inputFile)
    
    logger.debug(s"""loaded pig script from "$inputFile" """)

    val fileName = new File(inputFile).getName

    // 2. then we parse it and construct a dataflow plan
    var plan = new DataflowPlan(parseScriptFromSource(source, params))
    
    try {
      // if this does _not_ throw an exception, the schema is ok
      plan.checkSchemaConformance
    } catch {
      case e:SchemaException => {
        println(s"schema conformance error in ${e.getMessage}")
        return
      }
    }

    val scriptName = fileName.replace(".pig", "")
    if (!plan.checkConnectivity) {
      println(s"dataflow plan not connected")
      return
    }

    logger.debug("successfully created dataflow plan")
    
    // 3. now, we should apply optimizations
    
    val mm = new MaterializationManager
    plan = processMaterializations(plan, mm)
    plan = processPlan(plan)
    
    logger.debug("finished optimizations")

    if (FileTools.compileToJar(plan, scriptName, outDir, compileOnly, backend)) {
      if (!compileOnly) {
        // 4. and finally deploy/submit
        val jarFile = s"$outDir${File.separator}${scriptName}${File.separator}${scriptName}.jar"
//        val runner = FileTools.getRunner(backend)
        val runner = BackendManager.backendConf(backend).runnerClass
        
        logger.info(s"""starting job at "$jarFile" using backend "$backend" """)
        
        runner.execute(master, scriptName, jarFile)
      } else
        logger.info("successfully compiled program - exiting.")
    } else 
      logger.error("creating jar file failed")
  }

  def replaceParameters(line: String, params: Map[String,String]): String = {
    var s = line
    params.foreach{case (p, v) => s = s.replaceAll("\\$" + p, v)}
    s
  }

  private def parseScriptFromSource(source: Source, params: Map[String,String]): List[PigOperator] = {
    parseScript(
      if (params.nonEmpty)
        source.getLines().map(line => replaceParameters(line, params)).mkString("\n")
      else
        source.getLines().mkString("\n")
    )
  }
}
