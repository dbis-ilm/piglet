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

import java.io.{PrintStream, File}
import dbis.piglet.Piglet._
import dbis.piglet.op.{Display, PigOperator, Dump}
import dbis.piglet.parser.PigParser
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.plan.rewriting.Rewriter._
import dbis.piglet.plan.PrettyPrinter._
import dbis.piglet.schema.SchemaException
import dbis.piglet.tools.logging.LogLevel
import dbis.piglet.tools.{HDFSService, FileTools, Conf}
import dbis.piglet.backends.{BackendConf, BackendManager}
import dbis.piglet.plan.MaterializationManager
import dbis.piglet.plan.rewriting.Rewriter
import dbis.piglet.codegen.PigletCompiler

import jline.console.ConsoleReader

import scala.collection.mutable.ListBuffer
import java.nio.file.{Path, Paths}
import jline.console.history.FileHistory
import dbis.piglet.tools.Conf

import dbis.piglet.plan.MaterializationManager
import dbis.piglet.plan.rewriting.Rewriter

import scopt.OptionParser
import java.net.URI
import dbis.piglet.op.cmd.HdfsCmd
import dbis.piglet.tools.CliParams

sealed trait JLineEvent

case class Line(value: String, plan: ListBuffer[PigOperator]) extends JLineEvent

case object EmptyLine extends JLineEvent

case object EOF extends JLineEvent

/**
  * A singleton object implementing the REPL for Piglet.
  */
object PigletREPL extends dbis.piglet.tools.logging.PigletLogging {

  val defaultScriptName = "__my_script"

  private val consoleReader = new ConsoleReader()

  /**
    * A counter to make script names unique - it will be
    * set to the system time.
    */
  var scriptCounter: Long = 0

  /**
    * Returns the current script name. Multiple calls will return
    * the same name until nextScriptName is called.
    *
    * @return the script name
    */
  def scriptName(): String = defaultScriptName + scriptCounter

  /**
    * Creates and returns a new unique script name used also for the jar file.
    *
    * @return a name for the script
    */
  def nextScriptName(): String = {
    scriptCounter = System.currentTimeMillis()
    defaultScriptName + scriptCounter
  }

  /**
    * Checks if the given string contains a unbalanced number of strings. This is needed
    * to determine whether we can finish the statement.
    *
    * @param s the string to check
    * @return true if number of '{' is not equal to the number of '}'
    */
  private def unbalancedBrackets(s: String): Boolean = {
    val leftBrackets = s.count(_ == '{')
    val rightBrackets = s.count(_ == '}')
    leftBrackets != rightBrackets
  }

  /**
    * Returns true if the given string represents a command that can be directly
    * executed.
    *
    * @param s the statement string
    * @return true if the string is a command
    */
  private def isCommand(s: String): Boolean = {
    val cmdList = List("help", "describe", "dump", "display", "prettyprint", "rewrite", "quit", "fs")
    val line = s.toLowerCase
    cmdList.exists(cmd => line.startsWith(cmd))
  }

  /**
    * Processes a filesystem command using HDFSService.
    *
    * @param s the command
    * @return true if the execution was successful
    */
  private def processFsCmd(s: String): Boolean = {
    /* use the parser to obtain the HDFS command object
     * The processFsCmd method is called only 
     */
	  val op = PigParser.parseScript(s, resetSchema = true).head.asInstanceOf[HdfsCmd]

	  try {
	    HDFSService.process(op.cmd, op.params)  	    
    } catch {
      case ex: Throwable => println(s"error while executing fs command: ${ex.getMessage}")
    }
	  false
  }

  /**
    * The console handler for reading a line, setting the prompt, and
    * dealing with the history.
    *
    * @param handler an event handler for the commands
    */
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
          else if (!isCommand(line) && (!line.trim.endsWith(";") || unbalancedBrackets(lineBuffer))) {
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
      FileTools.recursiveDelete(defaultScriptName)
      logger.debug("flushing history file")
      consoleReader.getHistory.asInstanceOf[FileHistory].flush()
    }
  }

  /**
    * Prints the usage string.
    */
  def usage: Unit = {
    println(
      """Commands:
        |<pig latin statement>; - See the PigLatin manual for details: http://hadoop.apache.org/pig
        |Diagnostic commands:
        |    describe <alias> - Show the schema for the alias.
        |    dump <alias> - Compute the alias and writes the results to stdout.
        |    prettyprint - Prints the dataflow plan operator list.
        |    rewrite - Rewrites the current dataflow plan.
        |
        |HDFS commands: fs <cmd> [args] , where <cmd> can be
        |    -ls [path] - list path content
        |    -rm path - remove file/directory at path
        |    -rmdir path - remove an empty directory at path
        |    -mkdir path - create directory at path
        |    -cat path - print the content of a file
        |    -copyToRemote src dest - upload local src file to dst on DFS
        |    -copyToLocal src dest - download HDFS src file to dst on local file system
        |    -getmerge src dst - download all parts stored in directory src to local file dst
        | All commands should accept same flags as their respective original HDFS command
        |
        |Utility Commands:
        |    help - Display this message.
        |    quit - Quit the Pig shell.
      """.stripMargin)
  }

  /**
    * Processes the REWRITE command.
    *
    * @param buf the list of PigOperators
    * @return
    */
  def handleRewrite(buf: ListBuffer[PigOperator]): Boolean = {
    val plan = new DataflowPlan(buf.toList)
    for (sink <- plan.sinkNodes) {
      println(pretty(sink))
      val newSink = processPigOperator(sink)
      println(pretty(newSink))
    }
    false
  }

  /**
    * Processes the PRETTYPRINT command.
    *
    * @param buf the list of PigOperators
    * @return
    */
  def handlePrettyPrint(buf: ListBuffer[PigOperator], profiling: Option[URI]): Boolean = {
    var plan = new DataflowPlan(buf.toList)

    if(profiling.isDefined) {
  	  val mm = new MaterializationManager(Conf.materializationBaseDir, profiling.get)
			plan = processMaterializations(plan, mm)
    }

    plan = processPlan(plan)

    plan.printPlan(0)
    false
  }

  /**
    * Processes the DESCRIBE command.
    *
    * @param s the input string
    * @param buf the list of PigOperators
    * @return false
    */
  def handleDescribe(s: String, buf: ListBuffer[PigOperator], profiling: Option[URI]): Boolean = {
    var plan = new DataflowPlan(buf.toList)

    if(profiling.isDefined) {
      val mm = new MaterializationManager(Conf.materializationBaseDir, profiling.get)
      plan = processMaterializations(plan, mm)
    }

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
            case Some(o) => println(o.schemaToString)
            case None => println(s"unknown alias '$alias'")
          }
          op_after_rewriting match {
            case Some(_) => op match {
              case Some(o) if o.schema != op_after_rewriting.get.schema =>
                val r_schema = op_after_rewriting.get.schema.toString
                println(s"After rewriting, '$alias''s schema is '$r_schema'.")
              case _ => ()
            }
            case None => println(s"Rewriting will remove '$alias'.")
          }
        case None => println("invalid describe command")
      }

    } catch {
      case e: SchemaException => Console.err.println(s"schema conformance error in ${e.getMessage}")
    }

    false
  }

  /**
    * Executes the Piglet script collected in buf.
    *
    * @param s the input string
    * @param buf the list of PigOperators
    * @return false
    */
  def executeScript(s: String, buf: ListBuffer[PigOperator], c: CliParams): Boolean = {
    try {
      if (s.toLowerCase.startsWith("dump ")) {
        // if we have multiple dumps in our script then only the first one
        // is executed. Thus, we have to remove all other DUMP statements in
        // our list of operators.
        val dumps = buf.filter(p => p.isInstanceOf[Dump])
        
        if(dumps.size > 1)
          logger.warn(s"Found ${dumps.size} DUMP commands - executing only the first one!")
        
        dumps.foreach(d => d.inputs.head.removeConsumer(d))
        buf --= dumps
      }
      // the same for DISPLAY
      if (s.toLowerCase.startsWith("display ")) {
        val displays = buf.filter(p => p.isInstanceOf[Display])
        
        if(displays.size > 1)
          logger.warn(s"Found ${displays.size} DISPLAY commands - executing only the first one!")
          
        displays.foreach(d => d.inputs.head.removeConsumer(d))
        buf --= displays
      }

      buf ++= PigParser.parseScript(s, resetSchema = false)
      var plan = new DataflowPlan(buf.toList)
      logger.debug("plan created.")

      if(c.profiling.isDefined) {
        val mm = new MaterializationManager(Conf.materializationBaseDir, c.profiling.get)
        plan = processMaterializations(plan, mm)
      }


      plan = processPlan(plan)
      logger.debug("plan rewritten.")

      try {
        // if this does _not_ throw an exception, the schema is ok
        plan.checkSchemaConformance
      } catch {
        case e: SchemaException => {
          logger.error(s"schema conformance error in ${e.getMessage} for plan")
          return false
        }
      }

   //      val jobJar = Paths.get(s"$backendPath/${Conf.backendJar(backend).toString}")

      nextScriptName()
      
      PigletCompiler.compilePlan(plan, scriptName, c) match {
        case Some(jarFile) =>
          val runner = BackendManager.backend.runnerClass
          runner.execute(c.master, scriptName, jarFile, c.backendArgs)
          FileTools.recursiveDelete(scriptName)

        case None => Console.err.println("failed to build jar file for job")
      }
    }
    catch {
      case e: Throwable =>
        Console.err.println(s"error while executing: ${e.getMessage}")
        e.printStackTrace(Console.err)
        FileTools.recursiveDelete(scriptName)
    }

    // buf.clear()
    false
  }

  /**
    * Processes the list of PigOperators and look for duplicate (i.e. redefined) pipes. In this case we
    * keep only the last one and eliminate all others.
    *
    * @param buf the original list of PigOperators
    */
  def eliminateDuplicatePipes(buf: ListBuffer[PigOperator]): Unit = {
    /*
      * Deletes all PigOperators from the list in the range [0, pos]
      * which have a pipe with the given name.
      *
      * @param pipe the pipe name we are looking for
      * @param pos the end position of the list to be processed
      * @param buf the list of PigOperators
      * @return the number of deleted operators
      */
    def deleteOperators(pipe: String, pos: Int, buf: ListBuffer[PigOperator]): Int = {
      var num = 0
      var i = 0
      var ppos = pos
      while (i < ppos) {
        val otherPipe = buf(i).outPipeName
        if (pipe == otherPipe) {
          buf.remove(i)
          num += 1
          ppos -= 1
        }
        else
          i += 1
      }
      num
    }

    // we start from the end of the list and look for operators producing pipes with the same name
    var i = buf.length-1
    while (i > 0) {
      val pipe = buf(i).outPipeName
      val offset = deleteOperators(pipe, i, buf)
      i -= (if (offset > 0) offset else 1)
    }
  }

  def main(args: Array[String]): Unit = {

    // parse cli params
    val c = CliParams.parse(args)
    
    println(s"Welcome to PigREPL ver. ${BuildInfo.version} (built at ${BuildInfo.builtAtString})")
    
    logger.setLevel(c.logLevel)
    
    logger debug s"""Running REPL with backend "${c.backend}" """

    BackendManager.init(c.backend)
    if (BackendManager.backend.raw)
      throw new NotImplementedError("RAW backends are currently not supported in REPL. Use PigCompiler instead!")


    if(c.profiling.isDefined) {
      val reachable = FileTools.checkHttpServer(c.profiling.get)

      if(! reachable) {
        logger.error(s"Statistics management server is not reachable at ${c.profiling.get}. Aborting")
        return
      }
    }


    console {
      case EOF => println("Ctrl-d"); true
      case Line(s, buf) if s.equalsIgnoreCase(s"quit") => true
      case Line(s, buf) if s.equalsIgnoreCase(s"help") => usage; false
      case Line(s, buf) if s.equalsIgnoreCase(s"prettyprint") => handlePrettyPrint(buf, c.profiling)
      case Line(s, buf) if s.equalsIgnoreCase(s"rewrite") => handleRewrite(buf)
      case Line(s, buf) if s.toLowerCase.startsWith(s"describe ") => handleDescribe(s, buf, c.profiling)
      case Line(s, buf) if s.toLowerCase.startsWith(s"dump ") ||
        s.toLowerCase.startsWith(s"display ") ||
        s.toLowerCase.startsWith(s"store ") ||
        s.toLowerCase.startsWith(s"socket_write ") => executeScript(s, buf, c)
      case Line(s, _) if s.toLowerCase.startsWith(s"fs ") => processFsCmd(s)
      case Line(s, buf) => try {
        buf ++= PigParser.parseScript(s, resetSchema = false)
        eliminateDuplicatePipes(buf)
        false
      } catch {
        case iae: IllegalArgumentException => println(iae.getMessage); false
      }
      case _ => false
    }
  }
}
