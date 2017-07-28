package dbis.piglet.tools

import java.nio.file.{Files, Path, StandardOpenOption}

import dbis.piglet.op.{PigOperator, TimingOp}
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.tools.logging.PigletLogging
//import guru.nidi.graphviz.engine.{Format, Graphviz}
//import guru.nidi.graphviz.parse.Parser

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration


case class Node(id: String, var time: Option[Duration] = None, var label: String = "") {
  
  private def mkLabel = {
    val t = if(time.isDefined) s"\n${time.get.toMillis}ms (${BigDecimal(time.get.toMillis / 1000.0).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble}s)" else ""
    val l = s"$label\n$id\n$t" 
    PlanWriter.quote(l)
  }
  
  override def toString = s"op$id ${if(label.trim().nonEmpty) s"[label=$mkLabel]" else ""}"
}

case class Edge(from: String, to: String, var label: String = "") {
  override def toString = s"op$from -> op$to ${if(label.trim().nonEmpty) s"[label=$label]" else "" }"
}

/**
  * A helper class to print the plan in the Graphviz Dot format.
  */
object PlanWriter extends PigletLogging {

  /**
    * The method to write the plan in Dot format
    * @param dir The directory to save the file to
    * @param scriptName File name (without extension)
    */
  def createImage(dir: Path, scriptName: String): Unit = {
    val graph = createDotGraph()
//    val g = Parser.read(graph)
//    Graphviz.fromGraph(g).render(Format.PNG).toFile(dir.resolve(s"$scriptName.png").toFile)
    writeDotFile(dir.resolve(s"$scriptName.dot"), graph)
  }

  /**
    * Helper method to properly quote node labels and other strings for Dot
    * @param s The string
    * @return Returns the modified string where all double quotes are replaced with single quotes
    */
  def quote(s: String) = s""""${s.replace('\"', '\'')}""""
  
  val nodes = mutable.Map.empty[String, Node]
  val edges = ListBuffer.empty[Edge]

  /**
    * Since [[TimingOp]]s have the same lineage string as the operator they are for,
    * we need to distinguish them here so that Dot does not see duplicate node IDs
    * @param op The operator
    * @return Returns the ID of the operator, based on its lineage signature
    */
  def signature(op: PigOperator) = op match {
      case _:TimingOp => s"timing_${op.lineageSignature}"
      case _ => op.lineageSignature
    }

  /**
    * Init the writer for a plan
    * @param plan The plan to write
    * @param includeTimingOp A flag to indicate whether timing operators should be added to the output
    */
  def init(plan: DataflowPlan, includeTimingOp: Boolean = true) = {
    
    val ops = if(includeTimingOp) plan.operators else plan.operators.filterNot(_.isInstanceOf[TimingOp])
    
    ops.foreach{ op =>
      val sig = signature(op)
      nodes += (sig -> Node(sig, label = quote(op.toString)))
      
      op.outputs.flatMap(_.consumer).foreach { c =>
        edges += Edge(sig, signature(c))
      }
    }
  }

  /**
    * Create the string representing the Dot representation of the plan
    * @return Returns the Dot representation of the plan as a string
    */
  private def createDotGraph(): String = s"""digraph {
                                    |  node [shape=box]
                                    |  ${nodes.values.mkString("\n")}
                                    |  ${edges.mkString("\n")}
                                    |}
                                  """.stripMargin

  /**
    * Write the graph to the specified file
    * @param file The file to save the graph into
    * @param graph The graph string to save
    */
  private def writeDotFile(file: Path, graph: String): Unit = {
    logger.info(s"writing dot file to $file")
    if(Files.notExists(file.getParent)) {
      Files.createDirectories(file.getParent)
    }
    Files.write(file, List(graph).asJava, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
  }
  
  
}