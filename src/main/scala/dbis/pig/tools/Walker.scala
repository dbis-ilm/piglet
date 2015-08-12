package dbis.pig.tools

import dbis.pig.op.PigOperator
import dbis.pig.op.PigOperator
import dbis.pig.op.PigOperator
import dbis.pig.op.PigOperator
import scala.collection.mutable.Stack
import dbis.pig.plan.DataflowPlan
import scala.collection.mutable.Queue
import scala.collection.mutable.Set

/**
 * A general trait to be implemented by all walkers
 */
trait Walker[T] {
  
  /**
   * Traverse the dataflow plan and apply the visitor at each node
   * 
   * @param plan The dataflow plan to traverse
   * @param visit The visitor function to apply for each node
   */
  def walk(plan: DataflowPlan)(visit: (T) => Unit): Unit
}

/**
 * This class provides the possibility to traverse a dataflow plan
 * in breadth first manner by starting at the sink nodes (botton-up)
 */
class BreadthFirstBottomUpWalker extends Walker[PigOperator] {
  
	@Override
	def walk(plan: DataflowPlan)(visit: (PigOperator => Unit)) = {
    
    // the list of unprocessed nodes
    val todo = Queue(plan.sinkNodes.toSeq: _*)
    
    // the list of already processed nodes
    val seen = Set.empty[String]
    
    while(!todo.isEmpty) {
      val op = todo.dequeue()
      
      val sig = op.lineageSignature 

      // if the signature of the current op has been seen before
      if(!seen.contains(sig)) {
    	  seen += sig  // mark as seen
    	  visit(op)    // apply the visitor to the current op
      }
      
      // add all producers of the current op to our todo list
      // execpt the ones that we have already seen
      todo ++= op.inputs.map(_.producer).filter { op => !seen.contains(op.lineageSignature) }
    }
  } 
}

/**
 * This class provides the possibility to traverse a dataflow plan
 * in breadth first manner by starting at the source nodes (top-down)
 */
class BreadthFirstTopDownWalker extends Walker[PigOperator] {
  
	@Override
	def walk(plan: DataflowPlan)(visit: (PigOperator => Unit)) = {
    
    // the list of unprocessed nodes
    val todo = Queue(plan.sourceNodes.toSeq: _*)
    
    // the list of already processed nodes
    val seen = Set.empty[String]
    
    while(!todo.isEmpty) {
      val op = todo.dequeue()
      
      val sig = op.lineageSignature 

      // if the signature of the current op has been seen before
      if(!seen.contains(sig)) {
    	  seen += sig  // mark as seen
    	  visit(op)    // apply the visitor to the current op
      }
      
      // add all consumers of the current op to our todo list
      // execpt the ones that we have already seen
      todo ++= op.outputs.flatMap(_.consumer).filter { op => !seen.contains(op.lineageSignature) }
    }
  } 
}
