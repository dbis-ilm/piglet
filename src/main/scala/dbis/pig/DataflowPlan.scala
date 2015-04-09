package dbis.pig

import scala.collection.mutable.Map
import scalax.collection.GraphEdge._
import scalax.collection.GraphPredef._
import scalax.collection.mutable.Graph

/**
 * Created by kai on 01.04.15.
 */
case class Pipe (name: String, producer: PigOperator)

case class InvalidPlanException(msg: String) extends Exception(msg)

class DataflowPlan(val operators: List[PigOperator]) {
  var pipes: Map[String, Pipe] = Map[String, Pipe]()
  var graph = Graph[PigOperator,DiEdge]()

  constructPlan(operators)

  def constructPlan(ops: List[PigOperator]) : Unit = {
    /*
     * 1. we create pipes for all outPipeNames and make sure they are unique
     */
    operators.foreach(op => {
      if (op.outPipeName != "") {
        if (pipes.contains(op.outPipeName))
          throw new InvalidPlanException("duplicate pipe: " + op.outPipeName)
        pipes(op.outPipeName) = Pipe(op.outPipeName, op)
      }
    })
    /*
     * 2. we assign the pipe objects to the input and output pipes of all operators
     *    based on their names
     */
    try {
      operators.foreach(op => {
        op.output = if (op.outPipeName != "") pipes.get(op.outPipeName) else None
        op.inputs = op.inPipeNames.map(p => pipes(p))
        op.constructSchema

        // println("op: " + op)
        /*
         * 3. while we process the operators we build a graph
         */
        op.inputs.foreach(p => graph += p.producer ~> op)
      })
    }
    catch {
      case e: java.util.NoSuchElementException => throw new InvalidPlanException("invalid pipe: " + e.getMessage)
    }
  }

  def sinkNodes: Set[PigOperator] = {
    graph.nodes.filter((n : Graph[PigOperator,DiEdge]#NodeT) => n.outDegree == 0).map(_.value).toSet[PigOperator]
  }

  def checkConnectivity: Boolean = {
    /*
     * because we have a graph the check is very easy
     */
    graph.isConnected
  }
 }
