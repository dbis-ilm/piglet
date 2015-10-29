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
import dbis.pig.parser.{LanguageFeature, PigParser}
import dbis.pig.plan.DataflowPlan
import dbis.pig.plan.rewriting.Rewriter._
import dbis.pig.plan.PrettyPrinter._
import dbis.pig.schema.SchemaException
import dbis.pig.tools.{HDFSService, FileTools, Conf}
import dbis.pig.backends.BackendManager
import dbis.pig.plan.MaterializationManager
import dbis.pig.plan.rewriting.Rewriter

import jline.console.ConsoleReader

import scala.collection.mutable.ListBuffer
import java.nio.file.{Path, Paths}
import jline.console.history.FileHistory
import dbis.pig.tools.Conf
import com.typesafe.scalalogging.LazyLogging

import dbis.pig.plan.MaterializationManager
import dbis.pig.plan.rewriting.Rewriter

import scopt.OptionParser

sealed trait JLineEvent
case class Line(value: String, plan: ListBuffer[PigOperator]) extends JLineEvent
case object EmptyLine extends JLineEvent
case object EOF extends JLineEvent

object PigREPL extends PigParser with LazyLogging {
  case class REPLConfig(master: String = "local",
                        outDir: String = ".",
                        backend: String = Conf.defaultBackend,
                        language: String = "pig",
                        interactive: Boolean = true,
                        backendArgs: Map[String,String] = Map())

  val consoleReader = new ConsoleReader()
  val defaultScriptName = "__my_script"

  def cleanupResult(dir: String): Unit = {
    import scalax.file.Path

    val path: Path = Path(dir)
    try {
      path.deleteRecursively(continueOnFailure = false)
    }
    catch {
      case e: java.io.IOException => // some file could not be deleted
    }

  }

  private def unbalancedBrackets(s: String): Boolean = {
    val leftBrackets = s.count(_ == '{')
    val rightBrackets = s.count(_ == '}')
    leftBrackets != rightBrackets
  }

  private def isCommand(s: String): Boolean = {
    val cmdList = List("help", "describe", "dump", "prettyprint", "rewrite", "quit", "fs")
    val line = s.toLowerCase
    cmdList.exists(cmd => line.startsWith(cmd))
  }

  private def processFsCmd(s: String): Boolean = {
    val sList = s.split(" ")
    val cmdList = sList.slice(1, sList.length)
    if (cmdList.head.startsWith("-")) {
      val paramList =
        if (cmdList.length == 1)
          List()
        else {
          val last = cmdList.last
          cmdList.slice(1, cmdList.length - 1).toList ::: List(if (last.endsWith(";")) last.substring(0, last.length - 1) else last)
        }
      HDFSService.process(cmdList.head.substring(1), paramList)
    }
    else
      println(s"invalid fs command '${cmdList.head}'")
    false
  }

  def console(handler: JLineEvent => Boolean) {
    var finished = false
    val planBuffer = ListBuffer[PigOperator]()
    var lineBuffer = ""
    var prompt = "pigsh> "
    var insideEmbeddedCode = false

    val history = new FileHistory(Conf.replHistoryFile.toFile().getAbsoluteFile)
    logger.debug(s"will use ${history.getFile} as history file")
    consoleReader.setHistory(history)  
    
    // avoid to handle "!" in special way
    consoleReader.setExpandEvents(false)

    try {
    while (!finished) {
      val line = consoleReader.readLine(prompt)
      if (line == null) {
        consoleReader.getTerminal().restore()
        consoleReader.shutdown
        finished = handler(EOF)
      }
      else if (line.size == 0) {
        finished = handler(EmptyLine)
      }
      else if (line.size > 0) {
        println("add to lineBuffer: " + line)
        lineBuffer += line
        if (line.startsWith("<%") || line.startsWith("<!")) {
          insideEmbeddedCode = true
          prompt = "    | "
        }
        else if (insideEmbeddedCode && (line.endsWith("%>") || line.endsWith("!>"))) {
          prompt = "pigsh> "
          finished = handler(Line(lineBuffer, planBuffer))
          lineBuffer = ""
        }

        // if the line doesn't end with a semicolon or the current
        // buffer contains a unbalanced number of brackets
        // then we change the prompt and do not execute the command.
        else if (!isCommand(line) && (! line.trim.endsWith(";") || unbalancedBrackets(lineBuffer))) {
          prompt = "    | "
        }
        else {
          finished = handler(Line(lineBuffer, planBuffer))
          prompt = "pigsh> "
          lineBuffer = ""
        }
      }
    }
    } finally {
      // remove directory $defaultScriptName
      cleanupResult(defaultScriptName)
      logger.debug("flushing history file")
      consoleReader.getHistory.asInstanceOf[FileHistory].flush()
    }
  }

  def usage: Unit = {
    consoleReader.println("""
        |Commands:
        |<pig latin statement>; - See the PigLatin manual for details: http://hadoop.apache.org/pig
        |Diagnostic commands:
        |    describe <alias> - Show the schema for the alias.
        |    dump <alias> - Compute the alias and writes the results to stdout.
        |    prettyprint - Prints the dataflow plan operator list.
        |    rewrite - Rewrites the current dataflow plan.
        |Utility Commands:
        |    help - Display this message.
        |    quit - Quit the Pig shell.
      """.stripMargin)
  }

  def main(args: Array[String]): Unit = {
    var master: String = "local"
    var outDir: Path = null
    var backend: String = Conf.defaultBackend
    var languageFeature = LanguageFeature.PlainPig
    var backendArgs: Map[String, String] = null
    var interactive: Boolean = true
    val parser = new OptionParser[REPLConfig]("PigShell") {
      head("PigShell", "0.2")
      opt[Unit]('i', "interactive") hidden() action { (_, c) => c.copy(interactive = true) } text ("start an interactive REPL")
      opt[String]('m', "master") optional() action { (x, c) => c.copy(master = x) } text ("spark://host:port, mesos://host:port, yarn, or local.")
      opt[String]('o',"outdir") optional() action { (x, c) => c.copy(outDir = x)} text ("output directory for generated code")
      opt[String]('b',"backend") optional() action { (x,c) => c.copy(backend = x)} text ("Target backend (spark, flink, ...)")
      opt[String]('l', "language") optional() action { (x,c) => c.copy(language = x)} text ("Accepted language (pig = default, sparql, streaming)")
      opt[Map[String,String]]("<backend-arguments>...") optional() action { (x, c) => c.copy(backendArgs = x) } text ("Pig script files to execute")
      help("help") text ("prints this usage text")
      version("version") text ("prints this version info")
    }
    parser.parse(args, REPLConfig()) match {
      case Some(config) => {
        // do stuff
        master = config.master
        outDir = Paths.get(config.outDir)
        backend = config.backend
        languageFeature = config.language match {
          case "sparql" => LanguageFeature.SparqlPig
          case "streaming" => LanguageFeature.StreamingPig
          case "pig" => LanguageFeature.PlainPig
          case _ => LanguageFeature.PlainPig
        }
        backendArgs = config.backendArgs
      }
      case None =>
        // arguments are bad, error message will have been displayed
        return
    }

   //  val backend = if(args.length==0) Conf.defaultBackend else args(0)
    
    logger debug s"""Running REPL with backend "$backend" """

    val backendConf = BackendManager.backend(backend)
    if(backendConf.raw)
      throw new NotImplementedError("RAW backends are currently not supported in REPL. Use PigCompiler instead!")

    BackendManager.backend = backendConf

    console {
      case EOF => println("Ctrl-d"); true
      case Line(s, buf) if s.equalsIgnoreCase(s"quit") => true
      case Line(s, buf) if s.equalsIgnoreCase(s"help") => usage; false
      case Line(s, buf) if s.equalsIgnoreCase(s"prettyprint") => {
        var plan = new DataflowPlan(buf.toList)
        
        val mm = new MaterializationManager
        plan = processMaterializations(plan, mm)
        plan = processPlan(plan)
        
        for(sink <- plan.sinkNodes) {
          println(pretty(sink))
        }
        false
      }
      case Line(s, buf) if s.equalsIgnoreCase(s"rewrite") => {
        val plan = new DataflowPlan(buf.toList)
        for (sink <- plan.sinkNodes) {
          println(pretty(sink))
          val newSink = processPigOperator(sink)
          println(pretty(newSink))
        }
        false
      }
      case Line(s, buf) if s.toLowerCase.startsWith(s"describe ") => {
        var plan = new DataflowPlan(buf.toList)
        val mm = new MaterializationManager
        plan = processMaterializations(plan, mm)

        try {
          plan.checkSchemaConformance
          
          val pat = "[Dd][Ee][Ss][Cc][Rr][Ii][Bb][Ee]\\s[A-Za-z]\\w*".r
          pat.findFirstIn(s) match {
            case Some(str) =>
              val alias = str.split(" ")(1)
              val op = plan.findOperatorForAlias(alias)
              plan = processPlan(plan)
              val op_after_rewriting = plan.findOperatorForAlias(alias)
              op match {
                case Some (op) => println (op.schemaToString)
                case None => println (s"unknown alias '$alias'")
              }
              op_after_rewriting match {
                case Some(_) => op match {
                  case Some(o) if (o.schema != op_after_rewriting.get.schema) =>
                    val r_schema = op_after_rewriting.get.schema.toString
                    println(s"After rewriting, '$alias''s schema is '$r_schema'.")
                  case _ => ()
                }
                case None => println(s"Rewriting will remove '$alias'.")
              }
            case None => println("invalid describe command")
          }
          
        } catch {
          case e:SchemaException => Console.err.println(s"schema conformance error in ${e.getMessage}")
        }

        false
      }
      case Line(s, buf) if (s.toLowerCase.startsWith(s"dump ") ||
                            s.toLowerCase().startsWith(s"store ") ||
                            s.toLowerCase.startsWith(s"socket_write "))=> {
        try {
          buf ++= parseScript(s, languageFeature)
          var plan = new DataflowPlan(buf.toList)

          val mm = new MaterializationManager
          plan = processMaterializations(plan, mm)
          plan = processPlan(plan)

          val templateFile = backendConf.templateFile
          val jobJar = Conf.backendJar(backend)

          FileTools.compilePlan(plan, defaultScriptName, Paths.get("."), false, jobJar, templateFile, backend) match {
            case Some(jarFile) =>
              val runner = backendConf.runnerClass
              runner.execute(master, defaultScriptName, jarFile, backendArgs)

            case None => Console.err.println("failed to build jar file for job")
          }
        }
        catch {
          case e : Throwable => 
            Console.err.println(s"error while executing: ${e.getMessage}")
            e.printStackTrace(Console.err)
        }

        // buf.clear()
        false
      }
      case Line(s, buf) if (s.toLowerCase().startsWith(s"fs ")) => {
        processFsCmd(s)
      }
      case Line(s, buf) => try {
        buf ++= parseScript(s, languageFeature)
        false
      } catch {
        case iae: IllegalArgumentException => println(iae.getMessage); false
      }
      case _ => false
    }
  }
}
