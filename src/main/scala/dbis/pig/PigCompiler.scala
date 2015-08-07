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
import dbis.pig.parser.LanguageFeature._
import dbis.pig.plan.DataflowPlan
import dbis.pig.plan.rewriting.Rewriter._
import dbis.pig.schema.SchemaException
import dbis.pig.tools.FileTools
import scopt.OptionParser
import scala.io.Source
import dbis.pig.plan.MaterializationManager
import dbis.pig.tools.Conf
import com.typesafe.scalalogging.LazyLogging
import java.nio.file.Path
import java.nio.file.Paths

object PigCompiler extends PigParser with LazyLogging {
  case class CompilerConfig(master: String = "local",
                            input: String = "",
                            compile: Boolean = false,
                            outDir: String = ".",
                            params: Map[String,String] = Map(),
                            backend: String = BuildSettings.backends.get("default").get("name"),
                            updateConfig: Boolean = false)

  def main(args: Array[String]): Unit = {
    var master: String = "local"
    var inputFile: Path = null
    var compileOnly: Boolean = false
    var outDir: Path = null
    var params: Map[String,String] = null
    var backend: String = null
    var updateConfig = false

    val parser = new OptionParser[CompilerConfig]("PigCompiler") {
      head("PigCompiler", "0.2")
      opt[String]('m', "master") optional() action { (x, c) => c.copy(master = x) } text ("spark://host:port, mesos://host:port, yarn, or local.")
      opt[Unit]('c', "compile") action { (_, c) => c.copy(compile = true) } text ("compile only (don't execute the script)")
      opt[String]('o',"outdir") optional() action { (x, c) => c.copy(outDir = x)} text ("output directory for generated code")
      opt[String]('b',"backend") optional() action { (x,c) => c.copy(backend = x)} text ("Target backend (spark, flink, ...)")
      opt[Map[String,String]]('p', "params") valueName("name1=value1,name2=value2...") action { (x, c) => c.copy(params = x) } text("parameter(s) to subsitute")
      opt[Unit]('u',"update-config") optional() action { (_,c) => c.copy(updateConfig = true) } text("update config file in $USER_HOME/.piglet") 
      help("help") text ("prints this usage text")
      version("version") text ("prints this version info")
      arg[String]("<file>") required() action { (x, c) => c.copy(input = x) } text ("Pig file")
    }
    // parser.parse returns Option[C]
    parser.parse(args, CompilerConfig()) match {
      case Some(config) => {
        // do stuff
        master = config.master
        inputFile = Paths.get(config.input)
        compileOnly = config.compile
        outDir = Paths.get(config.outDir)
        params = config.params
        backend = config.backend
        updateConfig = config.updateConfig
      }
      case None =>
        // arguments are bad, error message will have been displayed
        return
    }
    
    if(updateConfig)
    	Conf.copyConfigFile()
    
    // start processing
    run(inputFile, outDir, compileOnly, master, backend, params)
  }

  /**
   * Start compiling the Pig script into a the desired program
   */
  def run(inputFile: Path, outDir: Path, compileOnly: Boolean, master: String, backend: String, params: Map[String,String]): Unit = {
    
    // 1. we read the Pig file
    val source = Source.fromFile(inputFile.toFile())
    
    logger.debug(s"""loaded pig script from "$inputFile" """)

    val fileName = inputFile.getFileName

    // 2. then we parse it and construct a dataflow plan
    var plan = new DataflowPlan(parseScriptFromSource(source, params, backend))
    
    try {
      // if this does _not_ throw an exception, the schema is ok
      plan.checkSchemaConformance
    } catch {
      case e:SchemaException => {
        logger.error(s"schema conformance error in ${e.getMessage}")
        return
      }
    }

    val scriptName = fileName.toString().replace(".pig", "")
    if (!plan.checkConnectivity) {
      logger.error(s"dataflow plan not connected")
      return
    }

    logger.debug("successfully created dataflow plan")
    
    // 3. now, we should apply optimizations
    
    val mm = new MaterializationManager
    plan = processMaterializations(plan, mm)
    if (backend=="flinks") plan = processWindows(plan)
    plan = processPlan(plan)
    
    logger.debug("finished optimizations")

    FileTools.compileToJar(plan, scriptName, outDir, compileOnly, backend) match {
      // the file was created --> execute it
      case Some(jarFile) =>  
        if (!compileOnly) {
        // 4. and finally deploy/submit          
        val runner = FileTools.getRunner(backend)
        logger.debug(s"using runner class ${runner.getClass.toString()}")
        
        logger.info(s"""starting job at "$jarFile" using backend "$backend" """)
        
        runner.execute(master, scriptName, jarFile)
      } else
        logger.info("successfully compiled program - exiting.")
        
      case None => logger.error("creating jar file failed") 
    } 
      
  }

  def replaceParameters(line: String, params: Map[String,String]): String = {
    var s = line
    params.foreach{case (p, v) => s = s.replaceAll("\\$" + p, v)}
    s
  }

  private def parseScriptFromSource(source: Source, params: Map[String,String], backend: String): List[PigOperator] = {
      if (params.nonEmpty)
        if (backend == "flinks")
          parseScript(source.getLines().map(line => replaceParameters(line, params)).mkString("\n"), StreamingPig)
        else
          parseScript(source.getLines().map(line => replaceParameters(line, params)).mkString("\n"))
      else
        if (backend == "flinks")
          parseScript(source.getLines().mkString("\n"), StreamingPig)
        else
          parseScript(source.getLines().mkString("\n"))
  }
}
