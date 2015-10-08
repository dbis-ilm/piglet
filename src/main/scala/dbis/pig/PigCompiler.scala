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
import dbis.pig.backends.BackendManager
import java.nio.file.Path
import java.nio.file.Paths
import scala.collection.mutable.ListBuffer
import dbis.pig.backends.BackendConf


object PigCompiler extends PigParser with LazyLogging {
  
  case class CompilerConfig(master: String = "local",
                            inputs: Seq[File] = Seq.empty,
                            compile: Boolean = false,
                            outDir: String = ".",
                            params: Map[String,String] = Map(),
                            backend: String = Conf.defaultBackend,
                            updateConfig: Boolean = false,
                            backendArgs: Map[String, String] = Map()
                          )

  def main(args: Array[String]): Unit = {
    var master: String = "local"
    var inputFiles: Seq[Path] = null
    var compileOnly: Boolean = false
    var outDir: Path = null
    var params: Map[String,String] = null
    var backend: String = null
    var updateConfig = false
    var backendArgs: Map[String, String] = null

    val parser = new OptionParser[CompilerConfig]("PigCompiler") {
      head("PigCompiler", "0.2")
      opt[String]('m', "master") optional() action { (x, c) => c.copy(master = x) } text ("spark://host:port, mesos://host:port, yarn, or local.")
      opt[Unit]('c', "compile") action { (_, c) => c.copy(compile = true) } text ("compile only (don't execute the script)")
      opt[String]('o',"outdir") optional() action { (x, c) => c.copy(outDir = x)} text ("output directory for generated code")
      opt[String]('b',"backend") optional() action { (x,c) => c.copy(backend = x)} text ("Target backend (spark, flink, ...)")
      opt[Map[String,String]]('p', "params") valueName("name1=value1,name2=value2...") action { (x, c) => c.copy(params = x) } text("parameter(s) to subsitute")
      opt[Unit]('u',"update-config") optional() action { (_,c) => c.copy(updateConfig = true) } text(s"update config file in program home (see config file)")
      opt[Map[String,String]]("backend-args") valueName("key1==value1,key2=value2...") action { (x, c) => c.copy(backendArgs = x) } text("parameter(s) to subsitute")
      help("help") text ("prints this usage text")
      version("version") text ("prints this version info")
      arg[File]("<file>...") unbounded() required() action { (x, c) => c.copy(inputs = c.inputs :+ x) } text ("Pig script files to execute")
    }
    // parser.parse returns Option[C]
    parser.parse(args, CompilerConfig()) match {
      case Some(config) => {
        // do stuff
        master = config.master
        inputFiles = config.inputs.map { f => f.toPath() }  //Paths.get(config.input)
        compileOnly = config.compile
        outDir = Paths.get(config.outDir)
        params = config.params
        backend = config.backend
        updateConfig = config.updateConfig
        backendArgs = config.backendArgs
      }
      case None =>
        // arguments are bad, error message will have been displayed
        return
    }
    
    val files = inputFiles.takeWhile { p => !p.startsWith("-") }
//    val backendArgs = inputFiles.drop(files.size).map { p => p.toString() }.toArray
    
    /* IMPORTANT: This must be the first call to Conf
     * Otherwise, the config file was already loaded before we could copy the new one
     */
    if(updateConfig)
    	Conf.copyConfigFile()
    
    // start processing
    run(files, outDir, compileOnly, master, backend, params, backendArgs)
  }

  def run(inputFile: Path, outDir: Path, compileOnly: Boolean, master: String, backend: String, params: Map[String,String], backendArgs: Map[String,String]): Unit = {
    run(Seq(inputFile), outDir, compileOnly, master, backend, params, backendArgs)
  }
  
  /**
   * Start compiling the Pig script into a the desired program
   */
  def run(inputFiles: Seq[Path], outDir: Path, compileOnly: Boolean, master: String, backend: String, params: Map[String,String], backendArgs: Map[String,String]): Unit = {
    
    val backendConf = BackendManager.backend(backend)
    
    // XXX: Is this still needed?
    BackendManager.backend = backendConf
    
    if(backendConf.raw) {
      if(compileOnly) {
        logger.error("Raw backends do not support compile-only mode! Aborting")
        return
      }
      
      inputFiles.foreach { file => runRaw(file, master, backendConf, backendArgs) }
      
    } else {
      runWithCodeGeneration(inputFiles, outDir, compileOnly, master, backend, params, backendConf, backendArgs)
    }
  }
  
  def runRaw(file: Path, master: String, backendConf: BackendConf, backendArgs: Map[String,String]) {
    logger.debug(s"executing in raw mode: $file with master $master for backend ${backendConf.name} with arguments ${backendArgs.mkString(" ")}")    
    val runner = backendConf.runnerClass
    runner.executeRaw(file, master, backendArgs)
  }
  
  def runWithCodeGeneration(inputFiles: Seq[Path], outDir: Path, compileOnly: Boolean, master: String, backend: String, params: Map[String,String], backendConf: BackendConf, backendArgs: Map[String,String]) {
    logger.debug("start parsing input files")
    val schedule = ListBuffer.empty[(DataflowPlan,Path)]
    for(file <- inputFiles) {
      createDataflowPlan(file, params, backend) match {
        case Some(v) => schedule += ((v,file))
        case None => 
          logger.error(s"failed to create dataflow plan for $file - aborting")
          return        
      }
    }

    logger.debug("start processing created dataflow plans")
 

    val templateFile = backendConf.templateFile
    val jarFile = Conf.backendJar(backend)
    val mm = new MaterializationManager
   
    
    for(plan <- schedule) {
    
      // 3. now, we should apply optimizations
      var newPlan = plan._1
      newPlan = processMaterializations(newPlan, mm)
      if (backend=="flinks") newPlan = processWindows(newPlan)
      newPlan = processPlan(newPlan)
      
      logger.debug("finished optimizations")
      
  
      val scriptName = plan._2.getFileName.toString().replace(".pig", "")
      logger.debug(s"using script name: $scriptName")      
      
      FileTools.compilePlan(newPlan, scriptName, outDir, compileOnly, jarFile, templateFile, backend) match {
        // the file was created --> execute it
        case Some(jarFile) =>  
          if (!compileOnly) {
            // 4. and finally deploy/submit
            val runner = backendConf.runnerClass
            logger.debug(s"using runner class ${runner.getClass.toString()}")

            logger.info( s"""starting job at "$jarFile" using backend "$backend" """)
            runner.execute(master, scriptName, jarFile, backendArgs)
        } else
          logger.info("successfully compiled program - exiting.")
          
        case None => logger.error(s"creating jar file failed for ${plan._2}") 
      } 
    }
  }

  /**
   * Helper method to parse the given file into a dataflow plan
   * 
   * @param inputFile The file to parse
   * @param params Key value pairs to replace placeholders in the script
   * @param backend The name of the backend
   */
  def createDataflowPlan(inputFile: Path, params: Map[String,String], backend: String): Option[DataflowPlan] = {
      // 1. we read the Pig file
      val source = Source.fromFile(inputFile.toFile())
      
      logger.debug(s"""loaded pig script from "$inputFile" """)
  
      // 2. then we parse it and construct a dataflow plan
      val plan = new DataflowPlan(parseScriptFromSource(source, params, backend))
      

      try {
        // if this does _not_ throw an exception, the schema is ok
        plan.checkSchemaConformance
      } catch {
        case e:SchemaException => {
          logger.error(s"schema conformance error in ${e.getMessage} for plan $inputFile")
          return None
        }
      }
      
      if (!plan.checkConnectivity) {
        logger.error(s"dataflow plan not connected for $inputFile")
        return None
      }

      logger.debug(s"successfully created dataflow plan for $inputFile")

      return Some(plan)
    
  }

  /**
   * Replace placeholders in the script with values provided by the given map
   * 
   * @param line The line to process
   * @param params The map of placeholder key and the value to use as replacement
   */
  def replaceParameters(line: String, params: Map[String,String]): String = {
    var s = line
    params.foreach{case (p, v) => s = s.replaceAll("\\$" + p, v)}
    s
  }

  def loadScript(inputFile: Path): Iterator[String] = {
    logger.debug(s"""try to load pig script from "$inputFile" """)
    val source = Source.fromFile(inputFile.toFile())
    source.getLines()
  }

  /**
   * Handle IMPORT statements by simply replacing the line containing IMPORT with the content
   * of the imported file.
   *
   * @param lines the original script
   * @return the script where IMPORTs are replaced
   */
   def resolveImports(lines: Iterator[String]): Iterator[String] = {
    val buf: ListBuffer[String] = ListBuffer()
    for (l <- lines) {
      if (l.matches("""[ \t]*[iI][mM][pP][oO][rR][tT][ \t]*'([^'\p{Cntrl}\\]|\\[\\"bfnrt]|\\u[a-fA-F0-9]{4})*'[ \t\n]*;""")) {
        val s = l.split(" ")(1)
        val name = s.substring(1, s.length - 2)
        val path = Paths.get(name)
        val resolvedLine = resolveImports(loadScript(path))
        buf ++= resolvedLine
      }
      else
        buf += l
    }
    buf.toIterator
  }

  private def parseScriptFromSource(source: Source, params: Map[String,String], backend: String): List[PigOperator] = {
    /*
     * Handle IMPORT statements.
     */
    val sourceLines = resolveImports(source.getLines())
      if (params.nonEmpty) {
        /*
         * Replace placeholders by parameters.
         */
        /* TODO: can we provide a config value "language_feature" in the backend config ?
         * So we could parse this value and pass it here for all backends in the same way
         */
        if (backend == "flinks")
          parseScript(sourceLines.map(line => replaceParameters(line, params)).mkString("\n"), StreamingPig)
        else
          parseScript(sourceLines.map(line => replaceParameters(line, params)).mkString("\n"))
      }
      else {
        if (backend == "flinks")
          parseScript(sourceLines.mkString("\n"), StreamingPig)
        else
          parseScript(sourceLines.mkString("\n"))
      }
  }
}
