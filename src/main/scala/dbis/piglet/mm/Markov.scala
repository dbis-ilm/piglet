package dbis.piglet.mm

import dbis.piglet.Piglet.Lineage
import dbis.piglet.tools.logging.PigletLogging


import scalax.collection.mutable.Graph
import scalax.collection.edge.Implicits._
import scalax.collection.edge.WDiEdge
import scalax.collection.io.json._
import scalax.collection.io.json.descriptor.predefined.WDi


case class Op(lineage: Lineage, var cost: Option[T] = None) {
  override def equals(obj: scala.Any): Boolean = obj match {
    case Op(l,_) => l equals lineage
    case _ => false
  }

  override def hashCode(): Int = lineage.hashCode
}

object Op {
  implicit def lineageToOp(lineage: Lineage): Op = Op(lineage)
}

/**
  * A data structure to maintain profiling information on operator occurrences
  * @param adj The adjacency matrix
  */
case class Markov(protected[mm] var adj: Graph[Op, WDiEdge]) extends PigletLogging {


  import Markov._

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
  def add(parent: Op, child: Op): Unit = {

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

    val a = adj.get(Op(op)).inNeighbors.map(_.value.lineage).toList
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
  def weight(parent: Op, child: Op): Long =
    adj.find( (parent ~%> child)(0) ) // the weight is not regarded for search
        .map(_.weight)
        .getOrElse(0L)

  def cost(op: Op): Option[T] = adj.get(op).value.cost

  def totalCost(lineage: Lineage,
                probStrategy: Traversable[Long] => Double = Markov.ProbMin)(
                costStrategy: Traversable[(Long, Double)] => (Long, Double)): Option[(Long, Double)] = {

    val g = adj
    val sources = (g get startNode).outNeighbors
    val theOp: g.NodeT = g get lineage

    val paths = sources.map(_.pathTo(theOp))


    val runtimes = paths.map{ opath =>

      opath.map { p =>
        val costSum = p.nodes.map(_.cost) // for each node on the path from start to the target, get the cost
          .filter(_.isDefined) // only those costs that are defined
          .map(_.get.avg()) // get average cost per operator (could use min/max as well)
          .sum // sum up costs of operators


        val probs = p.edges.map(_.weight)
        val prob = probStrategy(probs)

        (costSum, prob)
      }
    }.filter(_.isDefined).map(_.get)

    val res = if(runtimes.isEmpty) None else Some(costStrategy(runtimes))

    res

  }

  def updateCost(lineage: Lineage, cost: Long) = {

    val a = adj.get( Op(lineage)).value
    if(a.cost.isDefined) {
      a.cost = Some(T.merge(a.cost.get, cost))
    } else
      a.cost = Some(T(cost))
  }

  /**
    * String representation of this model as JSON
    * @return JSON representation of this model
    */
  override def toString: String = adj.toJson(Markov.descriptor)

}

object Markov {

  val startNode: Op = Op("start")

  // used to serialize the nodes to JSON
  private val nodeDescriptor = new NodeDescriptor[Op](typeId = "operators"){
    def id(node: Any) = node match {
      case Op(l,_) => l
    }
  }

  // graph descriptor
  val descriptor = new Descriptor[Op](
    defaultNodeDescriptor = nodeDescriptor,
    defaultEdgeDescriptor = WDi.descriptor[Op]()
  )

  /**
    * Load the Markov model from JSON text representation
    * @param json The JSON representation of the graph
    * @return Returns the Markov model parsed from JSON
    */
  def fromJson(json: String): Markov = {
    val g = Graph.fromJson[Op, WDiEdge](json, descriptor)
    Markov(g)
  }

  /**
    * Create an empty model
    * @return An empty Markov model
    */
  def empty = new Markov(Graph[Op, WDiEdge]())


  def ProbMin(l: Traversable[(Long)]): Double = l.min
  def ProbMax(l: Traversable[(Long)]): Double = l.max
  def ProbProduct(l: Traversable[(Long)]): Double = l.product

  def CostMin(l: Traversable[(Long, Double)]): (Long, Double) = l.minBy(_._1)
  def CostMax(l: Traversable[(Long, Double)]): (Long, Double) = l.maxBy(_._1)
}

