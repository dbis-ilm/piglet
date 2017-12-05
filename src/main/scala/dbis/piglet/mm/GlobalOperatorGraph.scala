package dbis.piglet.mm

import dbis.piglet.Piglet.Lineage
import dbis.piglet.tools.logging.PigletLogging

import scalax.collection.edge.Implicits._
import scalax.collection.edge.WDiEdge
import scalax.collection.io.json._
import scalax.collection.io.json.descriptor.predefined.WDi
import scalax.collection.mutable.Graph

/**
  * Represents a node in the markov graph
  * @param lineage The lineage for the operator
  * @param cost The operator cost (execution time)
  */
case class Op(lineage: Lineage, var cost: Option[T] = None, var resultRecords: Option[Long] = None, var bytesPerRecord: Option[Double] = None) {
  override def equals(obj: scala.Any): Boolean = obj match {
    case Op(l,_,_,_) => l equals lineage
    case _ => false
  }

  override def hashCode(): Int = lineage.hashCode
}

object Op {
  def apply(lineage: Lineage, cost: T): Op = Op(lineage, Some(cost))

  implicit def lineageToOp(lineage: Lineage): Op = Op(lineage)
}

/**
  * A data structure to maintain profiling information on operator occurrences
  * @param model The graph representing operators with their costs and the number of transitions on the edges
  */
case class GlobalOperatorGraph(protected[mm] var model: Graph[Op, WDiEdge]) extends PigletLogging {

  import GlobalOperatorGraph._


  /* Increase the total amount of runs
   */
  def incrRuns() = add(startNode, startNode)

  /**
    * The total number of runs
    * @return Returns the number of runs
    */
  def totalRuns = rawWeight(startNode, startNode)


  /**
    * Add the information that there is an edge ([[dbis.piglet.op.Pipe]]) between parent and child
    * @param parent The "source" operator / producer
    * @param child  The "target" operator / consumer
    */
  def add(parent: Op, child: Op): Unit = {

    require(parent != child || (parent == startNode && child == startNode), s"an operator cannot be child of itself (parent must be != child): $parent")
    val newWeight = rawWeight(parent, child) + 1

    logger debug s"upserting edge $parent -> $child with new weight $newWeight"

    model upsert (parent ~%> child) (newWeight) // update with new weight / count
  }



//  private def _getparents(op: Lineage): Try[List[Lineage]] = Try {
//
//  }
  /**
    * Get the parent operators for the given operator
    * @param lineage The lineage / id of the operator to get the parents of.
    * @return Returns the list of parents of this operator, or None if there is no parent
    */
  def parents(lineage: Lineage): Option[List[Lineage]] = {
    model.find(Op(lineage)).map(o => o.inNeighbors.map{_.value.lineage}.toList).flatMap(l => if(l.isEmpty) None else Some(l))
  }

  def size = model.size

  /**
    * The weight of an edge representing the number of occurrences of this edge
    * @param parent The parent operator
    * @param child The child operator
    * @return Returns the weight of this edge or 0 if there is none
    */
  def rawWeight(parent: Op, child: Op): Long =
    model.find( (parent ~%> child)(0) ) // the weight is not regarded for search
      .map(_.weight)
      .getOrElse(0L)

  def outProbs(lineage: Lineage): Option[Double] =
    model.find(Op(lineage)).map{o =>

      val weigths = o.outgoing.map(e => e.weight)
      weigths.sum / weigths.size.toDouble

    }

  def cost(lineage: Lineage): Option[T] = model.find(Op(lineage)).flatMap(_.value.cost)

  def resultRecords(lineage: Lineage): Option[Long] = model.find(Op(lineage)).flatMap(_.value.resultRecords)
  def bytesPerRecord(lineage: Lineage): Option[Double] = model.find(Op(lineage)).flatMap(_.value.bytesPerRecord)

//  def outputRecords(lineage: Lineage): Option[Long] = model.find(Op(lineage)).flatMap(_.outNeighbors.headOption.flatMap(_.value.resultRecords))
//  def outputBytesPerRecord(lineage: Lineage): Option[Long] = model.find(Op(lineage)).flatMap(_.outNeighbors.headOption.flatMap(_.value.bytesPerRecord))


  /**
    * Get the cost (execution time and probability) from start to the specified node.
    *
    * The cost is determined by aggregating the cost from all operators on the path from the source to the specified
    * node (lineage). If the specified op (lineage) is the (indirect) result of a Join or Cross operation, we will
    * find multiple paths, i.e., one path for each source node.
    *
    * The strategy parameters allow to define the way how to aggregate the costs and probabilities on the found paths.
    * While the probStrategy is used only to aggregate the probabilities on a path, the costStrategy is used also to
    * decide which path defines that actual cost of the plan until the specified parameter-
    * @param lineage The operator
    * @param probStrategy The aggregation strategy for the probabilities
    * @param costStrategy The aggregation strategy for the costs of operators
    * @return Returns the total cost until the specified operator along with its probability,
    *         or None, if there is no such op
    */
  def totalCost(lineage: Lineage,
                probStrategy: Traversable[Double] => Double = GlobalOperatorGraph.ProbMin)(
                costStrategy: Traversable[(Long, Double)] => (Long, Double)): Option[(Long, Double)] = {

    // this assignment is needed by the type checker/compiler to bring the graph in scope
    val g = model

    // get all actual source (LOAD) nodes
    val sources = (g get startNode).outNeighbors

    //    logger.debug(s"found sources (${sources.size}): ${sources.mkString(",")}")

    // the node in the graph for the specified lineage
    val theOp: g.NodeT = g get lineage

    //    logger.debug(s"graph node for $lineage : $theOp ")

    /* The paths from all known source nodes to the op
     * An entry of None means that there is no path from that source to the op
     */
    val paths = sources.map(_.pathTo(theOp)).filter(_.isDefined).map(_.get).toList

//    logger.debug(s"paths from sources (${paths.size}): \n${paths.mkString("\n")}")

    val runtimes = paths.map{ path =>

        val costSum = path.nodes.map(_.cost) // for each node on the path from start to the target, get the cost
          .filter(_.isDefined) // only those costs that are defined
          .map(_.get.avg()) // get average cost per operator (could use min/max as well)
          .sum // sum up costs of operators

        // probabilities are stored as weights on the edges
        val probs = path.edges.map{e =>
          e.weight / e.head.outDegree.toDouble
        }
        // aggregate probabilities
        val prob = if(probs.isEmpty) Double.NaN else probStrategy(probs)

        (costSum, prob)
      }


    val res = if(runtimes.isEmpty) None else Some(costStrategy(runtimes))

    res

  }

  /**
    * Update the cost of an operator, e.g., after an additional execution.
    * The given cost value will be "merged" with the previous values
    * @param lineage The lineage of that operator
    * @param cost The newly measured cost
    */
  def updateCost(lineage: Lineage, cost: Long) = model.find( Op(lineage)).map(_.value).foreach { a =>
    if(a.cost.isDefined) {
      a.cost = Some(T.merge(a.cost.get, cost))
    } else
      a.cost = Some(T(cost))
  }

  def updateSize(size: SizeInfo) =model.find(Op(size.lineage)).map(_.value).foreach {a =>
      a.resultRecords = Some(size.records)
      a.bytesPerRecord = Some(size.bytes)
  }

  /**
    * String representation of this model as JSON
    * @return JSON representation of this model
    */
  def toJson: String = model.toJson(GlobalOperatorGraph.descriptor)

  override def toString: Lineage = model.toString()

}

object GlobalOperatorGraph {

  val startLineage: Lineage = "start"
  val startNode: Op = Op(startLineage)


  // used to serialize the nodes to JSON
  private val nodeDescriptor = new NodeDescriptor[Op](typeId = "operators"){
    def id(node: Any) = node match {
      case Op(l,_,_,_) => l
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
  def fromJson(json: String): GlobalOperatorGraph = {
    val g = Graph.fromJson[Op, WDiEdge](json, descriptor)
    GlobalOperatorGraph(g)
  }

  /**
    * Create an empty model
    * @return An empty Markov model
    */
  def empty = new GlobalOperatorGraph(Graph[Op, WDiEdge]())


  def ProbMin(l: Traversable[Double]): Double = l.min
  def ProbMax(l: Traversable[Double]): Double = l.max
  def ProbProduct(l: Traversable[Double]): Double = l.product
  def ProbAvg(l: Traversable[Double]): Double = l.sum / l.size

  def CostMin(l: Traversable[(Long, Double)]): (Long, Double) = l.minBy(_._1)
  def CostMax(l: Traversable[(Long, Double)]): (Long, Double) = l.maxBy(_._1)
}

