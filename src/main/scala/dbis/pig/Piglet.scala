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


import java.nio.file.Path

import dbis.pig.op.PigOperator
import dbis.pig.parser.PigParser
import dbis.pig.parser.LanguageFeature
import dbis.pig.plan.DataflowPlan
import dbis.pig.plan.rewriting.Rewriter._
import dbis.pig.plan.rewriting.Rules
import dbis.pig.schema.SchemaException
import dbis.pig.tools.FileTools
import dbis.pig.plan.MaterializationManager
import dbis.pig.tools.Conf
import dbis.pig.backends.BackendManager
import dbis.pig.backends.BackendConf
import dbis.pig.tools.DBConnection
import dbis.pig.tools.{DepthFirstTopDownWalker, BreadthFirstTopDownWalker}
import dbis.pig.mm.{DataflowProfiler, MaterializationPoint}
import dbis.pig.codegen.PigletCompiler
import dbis.pig.tools.logging.PigletLogging
import dbis.pig.tools.logging.LogLevel
import dbis.pig.tools.logging.LogLevel._

import java.io.File

import scopt.OptionParser
import scala.io.Source

import java.nio.file.Path
import java.nio.file.Paths
import scala.collection.mutable.ListBuffer

import scala.collection.mutable.{Map => MutableMap}


import scalikejdbc._

object Piglet extends PigletLogging {

  case class CompilerConfig(master: String = "local",
                            inputs: Seq[File] = Seq.empty,
                            compile: Boolean = false,
                            outDir: String = ".",
                            params: Map[String, String] = Map(),
                            backend: String = Conf.defaultBackend,
                            backendPath: String = ".",
                            language: String = "pig",
                            updateConfig: Boolean = false,
                            showPlan: Boolean = false,
                            backendArgs: Map[String, String] = Map(),
                            profiling: Boolean = false,
                            loglevel: Option[String] = None
                           )

  var master: String = "local"
  var backend: String = null
  var backendPath: String = null
  var languageFeature = LanguageFeature.PlainPig
  var logLevel: Option[String] = None

  def main(args: Array[String]): Unit = {
    var master: String = "local"
    var inputFiles: Seq[Path] = null
    var compileOnly: Boolean = false
    var outDir: Path = null
    var params: Map[String, String] = null
    var updateConfig = false
    var showPlan = false
    var backendArgs: Map[String, String] = null
    var profiling = false

    val parser = new OptionParser[CompilerConfig]("PigletCompiler") {
      head("PigletCompiler", s"ver. ${BuildInfo.version} (built at ${BuildInfo.builtAtString})")
      opt[String]('m', "master") optional() action { (x, c) => c.copy(master = x) } text ("spark://host:port, mesos://host:port, yarn, or local.")
      opt[Unit]('c', "compile") action { (_, c) => c.copy(compile = true) } text ("compile only (don't execute the script)")
      opt[Boolean]("profiling") optional() action { (x, c) => c.copy(profiling = x) } text ("Switch on profiling")
      opt[String]('o', "outdir") optional() action { (x, c) => c.copy(outDir = x) } text ("output directory for generated code")
      opt[String]('b', "backend") optional() action { (x, c) => c.copy(backend = x) } text ("Target backend (spark, flink, sparks, ...)")
      opt[String]("backend_dir") optional() action { (x, c) => c.copy(backendPath = x) } text ("Path to the diretory containing the backend plugins")
      opt[String]('l', "language") optional() action { (x, c) => c.copy(language = x) } text ("Accepted language (pig = default, sparql, streaming)")
      opt[Map[String, String]]('p', "params") valueName ("name1=value1,name2=value2...") action { (x, c) => c.copy(params = x) } text ("parameter(s) to subsitute")
      opt[Unit]('u', "update-config") optional() action { (_, c) => c.copy(updateConfig = true) } text (s"update config file in program home (see config file)")
      opt[Unit]('s', "show-plan") optional() action { (_, c) => c.copy(showPlan = true) } text (s"show the execution plan")
      opt[String]('g', "log-level") optional() action { (x, c) => c.copy(loglevel = Some(x.toUpperCase())) } text ("Set the log level: DEBUG, INFO, WARN, ERROR")
      opt[Map[String, String]]("backend-args") valueName ("key1=value1,key2=value2...") action { (x, c) => c.copy(backendArgs = x) } text ("parameter(s) to subsitute")
      help("help") text ("prints this usage text")
      version("version") text ("prints this version info")
      arg[File]("<file>...") unbounded() required() action { (x, c) => c.copy(inputs = c.inputs :+ x) } text ("Pig script files to execute")
    }

    // parser.parse returns Option[C]
    parser.parse(args, CompilerConfig()) match {
      case Some(config) => {
        // do stuff
        master = config.master
        inputFiles = config.inputs.map { f => f.toPath() } //Paths.get(config.input)
        compileOnly = config.compile
        outDir = Paths.get(config.outDir)
        params = config.params
        backend = config.backend
        backendPath = config.backendPath
        updateConfig = config.updateConfig
        showPlan = config.showPlan
        backendArgs = config.backendArgs
        languageFeature = config.language match {
          case "sparql" => LanguageFeature.SparqlPig
          case "streaming" => LanguageFeature.StreamingPig
          case "pig" => LanguageFeature.PlainPig
          case _ => LanguageFeature.PlainPig
        }
        // note: for some backends we could determine the language automatically
        profiling = config.profiling
        logLevel = config.loglevel
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
    if (updateConfig)
      Conf.copyConfigFile()

    if (logLevel.isDefined) {
      try {
        logger.setLevel(LogLevel.withName(logLevel.get))
      } catch {
        case e: NoSuchElementException => println(s"ERROR: invalid log level ${logLevel} - continue with default")
      }
    }

    // start processing
    run(files, outDir, compileOnly, master, backend, languageFeature, params, backendPath, backendArgs, profiling, showPlan)
  }

  def run(inputFile: Path, outDir: Path, compileOnly: Boolean, master: String, backend: String,
          langFeature: LanguageFeature.LanguageFeature, params: Map[String, String], backendPath: String,
          backendArgs: Map[String, String], profiling: Boolean, showPlan: Boolean): Unit = {
    run(Seq(inputFile), outDir, compileOnly, master, backend, langFeature, params, backendPath, backendArgs, profiling, showPlan)
  }

  /**
    * Start compiling the Pig script into a the desired program
    */
  def run(inputFiles: Seq[Path], outDir: Path, compileOnly: Boolean, master: String, backend: String,
          langFeature: LanguageFeature.LanguageFeature, params: Map[String, String],
          backendPath: String, backendArgs: Map[String, String], profiling: Boolean, showPlan: Boolean): Unit = {

    try {
      // initialize database driver and connection pool
      if (profiling)
        DBConnection.init(Conf.databaseSetting)

      val backendConf = BackendManager.backend(backend)
      BackendManager.backend = backendConf

      if (backendConf.raw) {
        if (compileOnly) {
          logger.error("Raw backends do not support compile-only mode! Aborting")
          return
        }

        if (profiling) {
          logger.error("Raw backends do not support profiling yet! Aborting")
          return
        }

        inputFiles.foreach { file => runRaw(file, master, backendConf, backendArgs) }

      } else {
        runWithCodeGeneration(inputFiles, outDir, compileOnly, master, backend, langFeature, params, backendPath,
          backendConf, backendArgs, profiling, showPlan)
      }

    } catch {
      // don't print full stack trace to error
      case e: Exception =>
        logger.error(s"An error occured: ${e.getMessage}")
        logger.debug(e.toString)

    } finally {

      // close connection pool
      if (profiling)
        DBConnection.exit()
    }
  }

  /**
    *
    * @param file
    * @param master
    * @param backendConf
    * @param backendArgs
    */
  def runRaw(file: Path, master: String, backendConf: BackendConf, backendArgs: Map[String, String]) {
    logger.debug(s"executing in raw mode: $file with master $master for backend ${backendConf.name} with arguments ${backendArgs.mkString(" ")}")
    val runner = backendConf.runnerClass
    runner.executeRaw(file, master, backendArgs)
  }

  def runWithCodeGeneration(inputFiles: Seq[Path], outDir: Path, compileOnly: Boolean, master: String, backend: String,
                            langFeature: LanguageFeature.LanguageFeature, params: Map[String, String],
                            backendPath: String, backendConf: BackendConf, backendArgs: Map[String, String],
                            profiling: Boolean, showPlan: Boolean) {
    logger.debug("start parsing input files")
    
    var schedule = ListBuffer.empty[(DataflowPlan,Path)]
    
    for(file <- inputFiles) {
      PigletCompiler.createDataflowPlan(file, params, backend, langFeature) match {
        case Some(v) => schedule += ((v, file))
        case None =>
          logger.error(s"failed to create dataflow plan for $file - aborting")
          return
      }
    }

    // TODO: make cmdline arg
    val merge = true
    if(merge) {
      logger.debug("Start merging plans")
      
      val mergedPlan = mergePlans(schedule.map{case (plan, _) => plan })
      schedule = ListBuffer((mergedPlan, Paths.get(s"merged_${System.currentTimeMillis()}.pig")))  
    }
    
    //////////////////////////////////////
    // begin global analysis phase

    if (profiling)
      analyzePlans(schedule)

    ///////////////////////////////////////////
    // end analysis and continue normal processing

    logger.debug("start processing created dataflow plans")


    val templateFile = backendConf.templateFile
    val jarFile = Conf.backendJar(backend)
    val mm = new MaterializationManager

    val profiler = new DataflowProfiler

    for ((plan, path) <- schedule) {

      // 3. now, we should apply optimizations
      var newPlan = plan

      if (profiling)
        newPlan = processMaterializations(newPlan, mm)


      if (langFeature == LanguageFeature.StreamingPig && backend == "flinks")
        newPlan = processWindows(newPlan)

      Rules.registerBackendRules(backend)
      newPlan = processPlan(newPlan)
      // find materialization points

      if (profiling) {
        val walker = new DepthFirstTopDownWalker
        walker.walk(newPlan) { op =>

          logger.debug( s"""checking database for runtime information for operator "${op.lineageSignature}" """)
          // check for the current operator, if we have some runtime/stage information 
          val avg = DB readOnly { implicit session =>
            sql"select avg(progduration) as pd, avg(stageduration) as sd from exectimes where lineage = ${op.lineageSignature} group by lineage"
              .map { rs => (rs.long("pd"), rs.long("sd")) }.single().apply()
          }

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
              0L, // TODO: calculate saving time at this point
              0 // no count/size information so far
            )

            profiler.addMaterializationPoint(mp)

          } else {
            logger.debug(s" found no runtime information")
          }
        }
      }


      logger.debug("finished optimizations")

      if (showPlan) {
        println("final plan = {")
        newPlan.printPlan()
        println("}")
      }

      try {
        // if this does _not_ throw an exception, the schema is ok
        // TODO: we should do this AFTER rewriting!
        newPlan.checkSchemaConformance
      } catch {
        case e: SchemaException => {
          logger.error(s"schema conformance error in ${e.getMessage} for plan")
          return
        }
      }

      val scriptName = path.getFileName.toString().replace(".pig", "")
      logger.debug(s"using script name: $scriptName")

      PigletCompiler.compilePlan(newPlan, scriptName, outDir, Paths.get(s"$backendPath/${jarFile.toString}"),
        templateFile, backend, profiling) match {
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

        case None => logger.error(s"creating jar file failed for ${path}")
      }
    }
  }

  
  
  private def analyzePlans(schedule: Seq[(DataflowPlan, Path)]) {

    logger.debug("start creating lineage counter map")

    val walker = new BreadthFirstTopDownWalker

    val lineageMap = MutableMap.empty[String, Int]

    def visitor(op: PigOperator): Unit = {
      val lineage = op.lineageSignature
      //      logger.debug(s"visiting: $lineage")
      var old = 0
      if (lineageMap.contains(lineage))
        old = lineageMap(lineage)

      lineageMap(lineage) = old + 1
    }

    schedule.foreach { plan => walker.walk(plan._1)(visitor) }

    val entrySet = lineageMap.map { case (k, v) => Seq('id -> k, 'cnt -> v) }.toSeq


    DB autoCommit { implicit session =>
      sql"create table if not exists opcount(id varchar(200) primary key, cnt int default 0)"
        .execute
        .apply()
    }

    DB localTx { implicit session =>

      sql"merge into opcount(id,cnt) select {id}, case when exists(select 1 from opcount where id={id}) then (select cnt from opcount where id={id})+{cnt} else 1 end"
        .batchByName(entrySet: _ *)
        .apply()

    }
    
//    val entries = DB readOnly { implicit session => 
//      sql"select * from opcount"
//        .map{ rs => s"${rs.string("id")}  -->  ${rs.int("cnt")}" }
//        .list
//        .apply()
//      
//    }
//    entries.foreach { println }
    
  }

  /**
    * Sets the various configuration parameters to the given string values.
    *
    * @param master the master for Spark/Flink
    * @param backend the backend used for execution (spark, flink, sparks, flinks, ...)
    * @param language the Piglet dialect used for processing the script
    * @param backendDir the directory where the backend-specific jars are located
    */
  def setConfig(master: String = "local", backend: String = "spark", language: String = "pig",
                backendDir: String = "."): Unit = {
    Piglet.master = master
    Piglet.backend = backend
    Piglet.backendPath = backendDir
    Piglet.languageFeature = language match {
      case "sparql" => LanguageFeature.SparqlPig
      case "streaming" => LanguageFeature.StreamingPig
      case "pig" => LanguageFeature.PlainPig
      case _ => LanguageFeature.PlainPig
    }
    val backendConf = BackendManager.backend(backend)
    BackendManager.backend = backendConf
  }

  /**
    * Compiles and executes the given Piglet script.
    *
    * @param fileName the file name of the Piglet script
    */
  def compileFile(fileName: String): Unit = {
    val path = Paths.get(fileName)
    PigletCompiler.createDataflowPlan(path, Map[String, String](), backend, languageFeature) match {
      case Some(p) => compileAndExecute(p)
      case None => {}
    }
  }

  /**
    * Compiles and executes the given Piglet script represented as string.
    *
    * @param source a string containing the Piglet code
    */
  def compile(source: String): List[Any] = {
    PigletCompiler.createDataflowPlan(source, Map[String, String](), backend, languageFeature) match {
      case Some(p) => {
        val res = compileAndExecute(p)
        res.split("\n").toList.map(_.split(","))
      }
      case None => List()
    }
  }

  private def compileAndExecute(p: DataflowPlan): String = {
    def cleanup(s: String): Unit = {
      import scalax.file.Path

      val path: Path = Path(s)
      try {
        path.deleteRecursively(continueOnFailure = false)
      }
      catch {
        case e: java.io.IOException => // some file could not be deleted
      }
    }

    val plan = processPlan(p)
    plan.checkSchemaConformance

    val backendConf = BackendManager.backend
    val outDir = Paths.get(".")
    val scriptName = "__r_piglet"
    val templateFile = backendConf.templateFile
    val jarFile = Conf.backendJar(backend)
    val res: String = PigletCompiler.compilePlan(plan, scriptName, outDir, Paths.get(s"$backendPath/${jarFile.toString}"),
      templateFile, backend, false) match {
      case Some(jarFile) => {
        val runner = backendConf.runnerClass
        logger.debug(s"using runner class ${runner.getClass.toString()}")

        logger.info( s"""starting job at "$jarFile" using backend "$backend" """)
        val resStream = new java.io.ByteArrayOutputStream
        Console.withOut(resStream)(runner.execute(master, scriptName, jarFile, Map[String, String]()))
        resStream.toString
      }
      case None => ""
    }
    cleanup(scriptName)
    res
  }

}
