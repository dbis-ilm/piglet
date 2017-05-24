package dbis.piglet.mm

import dbis.piglet.Piglet.Lineage
import dbis.piglet.tools.logging.PigletLogging

import scalax.collection.mutable.Graph
import scalax.collection.edge.Implicits._
import scalax.collection.edge.WDiEdge
import scalax.collection.io.json._
import scalax.collection.io.json.descriptor.predefined.WDi

/**
  * A data structure to maintain profiling information on operator occurrences
  * @param adj The adjacency matrix
  */
case class Markov(protected[mm] var adj: Graph[Lineage, WDiEdge]) extends PigletLogging {


  private val startNode: Lineage = "start"

  /* Increase the total amount of runs
   */
  def incrRuns() = add(startNode, startNode)

  /**
    * The total number of runs
    * @return Returns the number of runs
    */
  def totalRuns = weight(startNode, startNode)

  /**
    * Add the information that there is an edge ([[dbis.piglet.op.Pipe]]) between parent and child
    * @param parent The "source" operator / producer
    * @param child  The "target" operator / consumer
    */
  def add(parent: Lineage, child: Lineage): Unit = {

    require(parent != child || (parent == startNode && child == startNode), "an operator cannot be child of itself (parent must be != child): $parent")
    val newWeight = weight(parent, child) + 1
    adj upsert (parent ~%> child) (newWeight) // update with new weight / count
  }

  /**
    * Get the parent operators for the given operator
    * @param op The lineage / id of the operator to get the parents of.
    * @return Returns the list of parents of this operator, or None if there is no parent
    */
  def parents(op: Lineage): Option[List[Lineage]] = {

    val a = adj.get(op).inNeighbors.map(_.value).toList
    if(a.isEmpty)
      None
    else
      Some(a)
  }

  def size = adj.size

  /**
    * The weight of an edge representing the number of occurrences of this edge
    * @param parent The parent operator
    * @param child The child operator
    * @return Returns the weight of this edge or 0 if there is none
    */
  def weight(parent: Lineage, child: Lineage): Long =
    adj.find( (parent ~%> child)(0) ) // the weight is not regarded for search
        .map(_.weight)
        .getOrElse(0L)

  /**
    * String representation of this model as JSON
    * @return JSON representation of this model
    */
  override def toString: String = adj.toJson(Markov.descriptor)

}

object Markov {

  // used to serialize the nodes to JSON
  private val nodeDescriptor = new NodeDescriptor[Lineage](typeId = "operators"){
    def id(node: Any) = node match {
      case l: Lineage => l
    }
  }

  // graph descriptor
  val descriptor = new Descriptor[Lineage](
    defaultNodeDescriptor = nodeDescriptor,
    defaultEdgeDescriptor = WDi.descriptor[Lineage]()
  )

  /**
    * Load the Markov model from JSON text representation
    * @param json The JSON representation of the graph
    * @return Returns the Markov model parsed from JSON
    */
  def fromJson(json: String): Markov = {
    val g = Graph.fromJson[Lineage, WDiEdge](json, descriptor)
    Markov(g)
  }

  /**
    * Create an empty model
    * @return An empty Markov model
    */
  def empty = new Markov(Graph[Lineage, WDiEdge]())
}

