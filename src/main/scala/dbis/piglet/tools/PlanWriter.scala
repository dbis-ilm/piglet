package dbis.piglet.tools

import dbis.piglet.plan.DataflowPlan
import java.nio.file.Path
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import dbis.piglet.tools.logging.PigletLogging
import scala.collection.mutable.Map
import dbis.piglet.op.TimingOp
import dbis.piglet.op.PigOperator


case class Node(id: String, var time: Option[Double] = None, var label: String = "") {
  
  private def mkLabel = {
    val t = if(time.isDefined) "\n"+time.get else ""
    val l = s"$label\n$id\n$t" 
    PlanWriter.quote(l)
  }
  
  override def toString() = s"op${id} ${if(label.trim().nonEmpty) s"[label=${mkLabel}]" else ""}"
}
case class Edge(from: String, to: String, var label: String = "") {
  override def toString() = s"op$from -> op$to ${if(label.trim().nonEmpty) s"[label=$label]" else "" }"
}


object PlanWriter extends PigletLogging {
  
  def quote(s: String) = s""""${s.replace('\"', '\'')}""""
  
  val nodes = Map.empty[String, Node]
  val edges = ListBuffer.empty[Edge]
  
  private def signature(op: PigOperator) = op match { 
      case _:TimingOp => s"timing_${op.lineageSignature}"
      case _ => op.lineageSignature
    }
  
  def init(plan: DataflowPlan, includeTimingOp: Boolean = true) = {
    
    val ops = if(includeTimingOp) plan.operators else plan.operators.filterNot(_.isInstanceOf[TimingOp])
    
    ops.foreach{ op =>
      val sig = signature(op)
      nodes += (sig -> Node(sig, label = quote(op.toString())))
      
      op.outputs.flatMap(_.consumer).foreach { c =>
        edges += Edge(sig, signature(c))
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