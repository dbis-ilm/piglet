/*
 * Copyright (c) 2015 The Piglet team,
 *                    All Rights Reserved.
 *
 * This file is part of the Piglet package.
 *
 * PipeFabric is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License (GPL) as
 * published by the Free Software Foundation; either version 2 of
 * the License, or (at your option) any later version.
 *
 * This package is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; see the file LICENSE.
 * If not you can find the GPL at http://www.gnu.org/copyleft/gpl.html
 */
package dbis.pig

import java.io.File
import dbis.pig.op.PigOperator
import dbis.pig.parser.PigParser
import dbis.pig.plan.DataflowPlan
import dbis.pig.schema.SchemaException
import dbis.pig.tools.FileTools
import jline.console.ConsoleReader

import scala.collection.mutable.ListBuffer

sealed trait JLineEvent
case class Line(value: String, plan: ListBuffer[PigOperator]) extends JLineEvent
case object EmptyLine extends JLineEvent
case object EOF extends JLineEvent

object PigREPL extends PigParser {
  val consoleReader = new ConsoleReader()

  def console(handler: JLineEvent => Boolean) {
    var finished = false
    val planBuffer = ListBuffer[PigOperator]()

    while (!finished) {
      val line = consoleReader.readLine("pigsh> ")
      if (line == null) {
        consoleReader.getTerminal().restore()
        consoleReader.shutdown
        finished = handler(EOF)
      }
      else if (line.size == 0) {
        finished = handler(EmptyLine)
      }
      else if (line.size > 0) {
        finished = handler(Line(line, planBuffer))
      }
    }
  }

  def usage: Unit = {
    consoleReader.println("""
        |Commands:
        |<pig latin statement>; - See the PigLatin manual for details: http://hadoop.apache.org/pig
        |Diagnostic commands:
        |    describe <alias> - Show the schema for the alias.
        |    dump <alias> - Compute the alias and writes the results to stdout.
        |Utility Commands:
        |    help - Display this message.
        |    quit - Quit the Pig shell.
      """.stripMargin)
  }

  def main(args: Array[String]): Unit = {
    val backend = if(args.length==0) BuildSettings.backends.get("default").get("name")
                  else { 
                    args(0) match{
                      case "flink" => "flink"
                      case "spark" => "spark"
                      case _ => throw new Exception("Unknown Backend $_")
                    }
                  }
    console {
      case EOF => println("Ctrl-d"); true
      case Line(s, buf) if s.equalsIgnoreCase(s"quit") => true
      case Line(s, buf) if s.equalsIgnoreCase(s"help") => usage; false
      case Line(s, buf) if s.toLowerCase.startsWith(s"describe ") => {
        val plan = new DataflowPlan(buf.toList)
        
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
      case Line(s, buf) if (s.toLowerCase.startsWith(s"dump ") || s.toLowerCase.startsWith(s"zmq_publisher "))=> {
        buf ++= parseScript(s)
        val plan = new DataflowPlan(buf.toList)
        if (FileTools.compileToJar(plan, "script", ".", false, backend)) {
          val jarFile = s".${File.separator}script${File.separator}script.jar"
//          val jarFile = "script.jar"
          val runner = FileTools.getRunner(backend)
          runner.execute("local", "script", jarFile)
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
