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
import dbis.pig.plan.PrettyPrinter._
import dbis.pig.schema.SchemaException
import dbis.pig.tools.FileTools
import jline.console.ConsoleReader
import scala.collection.mutable.ListBuffer
import java.nio.file.Paths
import jline.console.history.FileHistory
import dbis.pig.tools.Conf
import com.typesafe.scalalogging.LazyLogging
import org.slf4j.Marker
import dbis.pig.plan.MaterializationManager
import dbis.pig.plan.rewriting.Rewriter

sealed trait JLineEvent
case class Line(value: String, plan: ListBuffer[PigOperator]) extends JLineEvent
case object EmptyLine extends JLineEvent
case object EOF extends JLineEvent

object PigREPL extends PigParser with LazyLogging {
  val consoleReader = new ConsoleReader()
  
  private def unbalancedBrackets(s: String): Boolean = {
    val leftBrackets = s.count(_ == '{')
    val rightBrackets = s.count(_ == '}')
    leftBrackets != rightBrackets
  }

  private def isCommand(s: String): Boolean = {
    val cmdList = List("help", "describe", "dump", "prettyprint", "rewrite", "quit")
    val line = s.toLowerCase
    cmdList.exists(cmd => line.startsWith(cmd))
  }

  def console(handler: JLineEvent => Boolean) {
    var finished = false
    val planBuffer = ListBuffer[PigOperator]()
    var lineBuffer = ""
    var prompt = "pigsh> "

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
        // if the line doesn't end with a semicolon or the current
        // buffer contains a unbalanced number of brackets
        // then we change the prompt and do not execute the command.
        if (!isCommand(line) && (! line.trim.endsWith(";") || unbalancedBrackets(lineBuffer))) {
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
        |    prettyprint - Prints the dataflowplans operator list
        |    rewrite - Rewrites the current DataflowPlan
        |Utility Commands:
        |    help - Display this message.
        |    quit - Quit the Pig shell.
      """.stripMargin)
  }

  def main(args: Array[String]): Unit = {
    val backend = if(args.length==0) BuildSettings.backends.get("default").get("name")
                  else { 
                    args(0) match{
                      case "flink"  => "flink"
                      case "flinks" => "flinks"
                      case "spark"  => "spark"
                      case "sparks" => "sparks"
                      case _ => throw new Exception("Unknown Backend $_")
                    }
                  }
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
        plan = processPlan(plan)
        
        try {
          plan.checkSchemaConformance
          
          val pat = "[Dd][Ee][Ss][Cc][Rr][Ii][Bb][Ee]\\s[A-Za-z]\\w*".r
          pat.findFirstIn(s) match {
            case Some(str) =>
              val alias = str.split(" ")(1)
              plan.findOperatorForAlias(alias) match {
                case Some (op) => println (op.schemaToString)
                case None => println (s"unknown alias '$alias'")
              }
            case None => println("invalid describe command")
          }
          
        } catch {
          case e:SchemaException => println(s"schema conformance error in ${e.getMessage}")
        }
        
        false
      }
      case Line(s, buf) if (s.toLowerCase.startsWith(s"dump ") || s.toLowerCase.startsWith(s"socket_write "))=> {
        buf ++= parseScript(s)
        var plan = new DataflowPlan(buf.toList)
        
        val mm = new MaterializationManager
        plan = processMaterializations(plan, mm)
        plan = processPlan(plan)
        
        FileTools.compileToJar(plan, "script", Paths.get("."), false, backend) match {
          case Some(jarFile) =>
            val runner = FileTools.getRunner(backend)
            runner.execute("local", "script", jarFile)
          
          case None => println("failed to build jar file for job")
        }

        // buf.clear()
        false
      }
      case Line(s, buf) => try {
        buf ++= parseScript(s);
        false 
      } catch {
        case iae: IllegalArgumentException => println(iae.getMessage); false
      }
      case _ => false
    }
  }
}
