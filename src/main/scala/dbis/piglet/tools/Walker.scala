package dbis.piglet.tools

import dbis.piglet.op.PigOperator
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.tools.logging.PigletLogging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
object BreadthFirstBottomUpWalker extends Walker[PigOperator] {
  
	@Override
	def walk(plan: DataflowPlan)(visit: (PigOperator => Unit)) = {
    
    // the list of unprocessed nodes
    val todo = mutable.Queue(plan.sinkNodes.toSeq: _*)
    
    // the list of already processed nodes
    val seen = mutable.Set.empty[String]
    
    while(todo.nonEmpty) {
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

//class BreadthFirstTopDownIterator(plan: DataflowPlan) extends Iterator[PigOperator] {
//  
//  
//  // the list of unprocessed nodes
//  private val todo = Queue(plan.sourceNodes.toSeq: _*)
//  println(s"initial todo: ${todo.mkString("\n")}")
//  
//  // the list of already processed nodes
//  private val seen = Set.empty[String]
//  
//  override def hasNext = todo.nonEmpty
//  
//  override def next: PigOperator = {
//    
//    val op: PigOperator = todo.dequeue()
//    val sig = op.lineageSignature 
//
//    // if the signature of the current op has been seen before
//    if(!seen.contains(sig)) {
//  	  seen += sig  // mark as seen
//      // FIXME: at this point we know that we should return op
//      // if op was already seen, we need to jump to the next in the queue (but add the children to todo anyway)
//    }
//    
//    // add all consumers of the current op to our todo list
//    // execpt the ones that we have already seen
//    todo ++= op.outputs.flatMap(_.consumer).filter { op => !seen.contains(op.lineageSignature) }
//    op
//  }
//  
//  
//}

/**
 * This class provides the possibility to traverse a dataflow plan
 * in breadth first manner by starting at the source nodes (top-down)
 */
object BreadthFirstTopDownWalker extends Walker[PigOperator] {
  
	override def walk(plan: DataflowPlan)(visit: (PigOperator => Unit)) = {
//	  new BreadthFirstTopDownIterator(plan).foreach(visit)
	  val todo = mutable.Queue(plan.sourceNodes.toSeq: _*)
//    val seen = Set.empty[String]
	  val seen = mutable.Set.empty[PigOperator]
   
    while(todo.nonEmpty) {
      val op = todo.dequeue()      
//      val sig = op.lineageSignature 
  
      // if the signature of the current op has been seen before
      if(!seen.contains(op)) {
    	  seen += op  // mark as seen
    	  visit(op)    // apply the visitor to the current op
      }
      
      // add all consumers of the current op to our to-do-list
      // execpt the ones that we have already seen
      todo ++= op.outputs.flatMap(_.consumer).filter { op => !seen.contains(op) }
    }
  } 
}

object DepthFirstTopDownWalker extends Walker[PigOperator] {
  
  @Override
  override def walk(plan: DataflowPlan)(visit: (PigOperator => Unit)) = {
    
    val todo = mutable.Stack(plan.sourceNodes.toSeq: _*)
    val seen = mutable.Set.empty[PigOperator]
   
    while(todo.nonEmpty) {
      val op = todo.pop()
      
  
      // if the signature of the current op has been seen before
      if(!seen.contains(op)) {
        seen += op  // mark as seen
        visit(op)    // apply the visitor to the current op
      }
      
      val children = op.outputs.flatMap(_.consumer).filterNot { o => seen.contains(o) }
      todo.pushAll(children)
    }
  }
}

object TopoSort extends PigletLogging {
	
  def apply(plan: DataflowPlan) = {
    
    val l = ListBuffer.empty[PigOperator]
    val s = mutable.Queue(plan.sourceNodes.toSeq: _*)
    
    val m = mutable.Map.empty[PigOperator, Int].withDefaultValue(0)
    
    while(s.nonEmpty) {
      val n = s.dequeue()
      
      if(!l.contains(n))
    	  l.append(n)
    	  
      for(consumer <- n.outputs.flatMap(_.consumer)) {
        
        m(consumer) += 1
        if(m(consumer) >= consumer.inputs.size)
          s.enqueue(consumer)
      }
     
    }

    if(l.size != plan.operators.size) {
      println(s"l: ${l.size}  vs ops ${plan.operators.size}")
      val diff = plan.operators.diff(l)
      diff.foreach(println)
    	throw new IllegalStateException("we lost some operators")
    }
    
    l
  }
}
