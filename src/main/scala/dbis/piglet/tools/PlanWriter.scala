package dbis.piglet.tools

import dbis.piglet.plan.DataflowPlan
import java.nio.file.Path
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import dbis.piglet.tools.logging.PigletLogging
import scala.collection.mutable.Map


case class Node(id: String, var time: Option[Long] = None, var label: String = "") {
  override def toString() = s"op${id} ${if(label.trim().nonEmpty) s"[label=$label]" else ""}"
}
case class Edge(from: String, to: String, var label: String = "") {
  override def toString() = s"op$from -> op$to ${if(label.trim().nonEmpty) s"[label=$label]" else "" }"
}


object PlanWriter extends PigletLogging {
  
  def quote(s: String) = s""""${s.replace('\"', '\'')}""""
  
  private val nodes = Map.empty[String, Node]
  private val edges = ListBuffer.empty[Edge]
  
  def init(plan: DataflowPlan) = {
    
    plan.operators.foreach{ op =>
      val sig = op.lineageSignature
      nodes += (sig -> Node(sig, label = quote(op.toString())))
      
      op.outputs.flatMap(_.consumer).foreach { c =>
        edges += Edge(sig, c.lineageSignature)
      }
    }
  }
  
  def writeDotFile(file: Path) = {
    
    val dot = s"""digraph {
    |  node [shape=box]
    |  ${nodes.values.mkString("\n")}
    |  ${edges.mkString("\n")}      
    |}
    """.stripMargin  

    logger.info(s"writing dot file to $file")
    Files.write(file, List(dot).toIterable.asJava, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
  }
  
  
}