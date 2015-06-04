package dbis.pig

import java.io.File
import jline.console.ConsoleReader

import scala.collection.mutable.ListBuffer

/**
 * Created by kai on 29.04.15.
 */
sealed trait JLineEvent
case class Line(value: String, plan: ListBuffer[PigOperator]) extends JLineEvent
case object EmptyLine extends JLineEvent
case object EOF extends JLineEvent

object PigREPL extends PigParser {
  val backend = BuildSettings.backends.get("default").get("name")
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
      case Line(s, buf) if s.toLowerCase.startsWith(s"dump ") => {
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
