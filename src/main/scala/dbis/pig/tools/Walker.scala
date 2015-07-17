package dbis.pig.tools

import dbis.pig.op.PigOperator
import dbis.pig.op.PigOperator
import dbis.pig.op.PigOperator
import dbis.pig.op.PigOperator
import scala.collection.mutable.Stack
import dbis.pig.plan.DataflowPlan
import scala.collection.mutable.Queue
import scala.collection.mutable.Set

trait Walker[T] {
  def walk(plan: DataflowPlan)(visit: (T) => Unit): Unit
}

class BreadthFirstBottomUpWalker extends Walker[PigOperator] {
  
	@Override
	def walk(plan: DataflowPlan)(visit: (PigOperator => Unit)) = {
    
    val todo = Queue(plan.sinkNodes.toSeq: _*)
    
    val seen = Set.empty[String]
    
    while(!todo.isEmpty) {
      val op = todo.dequeue()
      
      val sig = op.lineageSignature 

      if(!seen.contains(sig)) {
    	  seen += sig
    	  visit(op)
      }
      
      todo ++= op.inputs.map(_.producer).filter { op => !seen.contains(op.lineageSignature) }
    }
  } 
}