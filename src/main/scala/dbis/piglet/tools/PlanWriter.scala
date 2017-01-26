package dbis.piglet.tools

import dbis.piglet.plan.DataflowPlan
import java.nio.file.Path
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import dbis.piglet.tools.logging.PigletLogging

object PlanWriter extends PigletLogging {
  
  def quote(s: String) = s""""${s.replace('\"', ' ')}""""
  
  def writeDotFile(plan: DataflowPlan, file: Path) = {
    
    val dot = s"""digraph {
    |  node [shape=box]
    |  ${ plan.operators.map{ op => s"op${op.lineageSignature} [label=${quote(op.toString())}]" }.mkString("\n") }
    |  ${  
      val edges = ListBuffer.empty[String]
      BreadthFirstTopDownWalker.walk(plan) { op =>
        op.outputs.flatMap(_.consumer).foreach { c =>
          edges += s"  op${op.lineageSignature} -> op${c.lineageSignature}"
        }
      }
      edges.mkString("\n")      
    }
    |}
    """.stripMargin  

    logger.info(s"writing dot file to $file")
    Files.write(file, List(dot).toIterable.asJava, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
  }
  
  
}