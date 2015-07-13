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

object PigCompiler extends PigParser {
  case class CompilerConfig(master: String = "local", input: String = "", compile: Boolean = false, outDir: String = ".", backend: String = BuildSettings.backends.get("default").get("name"))

  def main(args: Array[String]): Unit = {
    var master: String = "local"
    var inputFile: String = null
    var compileOnly: Boolean = false
    var outDir: String = null
    var backend: String = null

    val parser = new OptionParser[CompilerConfig]("PigCompiler") {
      head("PigCompiler", "0.1")
      opt[String]('m', "master") optional() action { (x, c) => c.copy(master = x) } text ("spark://host:port, mesos://host:port, yarn, or local.")
      opt[Unit]('c', "compile") action { (_, c) => c.copy(compile = true) } text ("compile only (don't execute the script)")
      opt[String]('o',"outdir") optional() action { (x, c) => c.copy(outDir = x)} text ("output directory for generated code")
      opt[String]('b',"backend") optional() action { (x,c) => c.copy(backend = x)} text ("Target backend (spark, flink, ...)")
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
        backend = config.backend
      }
      case None =>
        // arguments are bad, error message will have been displayed
        return
    }
    
    // start processing
    run(inputFile, outDir, compileOnly, master, backend)
  }

  /**
   * Start compiling the Pig script into a the desired program
   */
  def run(inputFile: String, outDir: String, compileOnly: Boolean, master: String, backend: String): Unit = {
    
    // 1. we read the Pig file
    val source = Source.fromFile(inputFile)

    val fileName = new File(inputFile).getName

    // 2. then we parse it and construct a dataflow plan
    var plan = new DataflowPlan(parseScriptFromSource(source))
    
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

    // 3. now, we should apply optimizations
    
    val mm = new MaterializationManager
    plan = processMaterializations(plan, mm)
    plan = processPlan(plan)

    if (FileTools.compileToJar(plan, scriptName, outDir, compileOnly, backend)) {
      val jarFile = s"$outDir${File.separator}${scriptName}${File.separator}${scriptName}.jar"

      if (!compileOnly) {
        // 4. and finally deploy/submit
        val runner = FileTools.getRunner(backend)
        runner.execute(master, scriptName, jarFile)
      }
    }
  }

  private def parseScriptFromSource(source: Source): List[PigOperator] = {
    parseScript(source.getLines().mkString("\n"))
  }
}
