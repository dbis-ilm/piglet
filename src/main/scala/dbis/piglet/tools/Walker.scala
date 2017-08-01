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
  def walk(plan: DataflowPlan, startNodes: Seq[T] = Seq.empty)(visit: (T) => Unit): Unit

  def collect(plan: DataflowPlan, startNodes: Seq[T] = Seq.empty): Set[T]
}

/**
 * This class provides the possibility to traverse a dataflow plan
 * in breadth first manner by starting at the sink nodes (botton-up)
 */
object BreadthFirstBottomUpWalker extends Walker[PigOperator] {

  private def doIt(plan: DataflowPlan, startNodes: Seq[PigOperator] = Seq.empty, visitor: Option[(PigOperator => Unit)]): mutable.Set[PigOperator] = {
    val start = if(startNodes.isEmpty) plan.sinkNodes.toSeq else startNodes

    // the list of unprocessed nodes
    val todo = mutable.Queue(start:_*)

    // the list of already processed nodes
    val seen = mutable.Set.empty[PigOperator]

    while(todo.nonEmpty) {
      val op = todo.dequeue()

//      val sig = op.lineageSignature

      // if the signature of the current op has been seen before
      if(!seen.contains(op)) {
//        seen += sig  // mark as seen
        seen += op

        if(visitor.isDefined) {
          val visit = visitor.get
          visit(op)    // apply the visitor to the current op
        }
      }

      // add all producers of the current op to our to-do list
      // execpt the ones that we have already seen
      todo ++= op.inputs.map(pi =>
        pi.producer)
        .filterNot { op => op == null || seen.contains(op) }
    }

    seen
  }


	override def walk(plan: DataflowPlan, startNodes: Seq[PigOperator] = Seq.empty)(visit: (PigOperator => Unit)) = doIt(plan, startNodes, Some(visit))

  override def collect(plan: DataflowPlan, startNodes: Seq[PigOperator]): Set[PigOperator] = doIt(plan, startNodes, None).toSet

}


/**
 * This class provides the possibility to traverse a dataflow plan
 * in breadth first manner by starting at the source nodes (top-down)
 */
object BreadthFirstTopDownWalker extends Walker[PigOperator] {

  private def doIt(plan: DataflowPlan, startNodes: Seq[PigOperator] = Seq.empty, visitor: Option[(PigOperator => Unit)]): mutable.Set[PigOperator] = {
    val start = if(startNodes.isEmpty) plan.sourceNodes.toSeq else startNodes

    val todo = mutable.Queue(start: _*)
    //    val seen = Set.empty[String]
    val seen = mutable.Set.empty[PigOperator]

    while(todo.nonEmpty) {
      val op = todo.dequeue()
      //      val sig = op.lineageSignature

      // if the signature of the current op has been seen before
      if(!seen.contains(op)) {
        seen += op  // mark as seen
        if(visitor.isDefined) {
          val visit = visitor.get
          visit(op)    // apply the visitor to the current op
        }
      }

      // add all consumers of the current op to our to-do-list
      // execpt the ones that we have already seen
      todo ++= op.outputs.flatMap(_.consumer).filter { op => !seen.contains(op) }
    }

    seen
  }


	override def walk(plan: DataflowPlan, startNodes: Seq[PigOperator] = Seq.empty)(visit: (PigOperator => Unit)) = doIt(plan, startNodes, Some(visit))

  override def collect(plan: DataflowPlan, startNodes: Seq[PigOperator]): Set[PigOperator] = doIt(plan, startNodes, None).toSet
}

object DepthFirstTopDownWalker extends Walker[PigOperator] {
  
  def doIt(plan: DataflowPlan, startNodes: Seq[PigOperator] = Seq.empty, visitor: Option[(PigOperator => Unit)]): mutable.Set[PigOperator] = {
    val start = if(startNodes.isEmpty) plan.sourceNodes.toSeq else startNodes

    val todo = mutable.Stack(start: _*)
    val seen = mutable.Set.empty[PigOperator]

    while(todo.nonEmpty) {
      val op = todo.pop()


      // if the signature of the current op has been seen before
      if(!seen.contains(op)) {
        seen += op  // mark as seen

        if(visitor.isDefined) {
          val visit = visitor.get
          visit(op)    // apply the visitor to the current op
        }
      }

      val children = op.outputs.flatMap(_.consumer).filterNot { o => seen.contains(o) }
      todo.pushAll(children)
    }

    seen
  }


  override def walk(plan: DataflowPlan, startNodes: Seq[PigOperator] = Seq.empty)(visit: (PigOperator => Unit)) = doIt(plan, startNodes, Some(visit))

  override def collect(plan: DataflowPlan, startNodes: Seq[PigOperator]): Set[PigOperator] = doIt(plan, startNodes, None).toSet
}

object TopoSort {
	
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
      val diff = plan.operators.diff(l)

      println(s"l: ${l.size}  vs ops ${plan.operators.size}: ${diff.mkString("\n")}")

      plan.operators.foreach(println)
      println("---")
      l.foreach(println)

      if(l.size > plan.operators.size)
        throw new IllegalStateException("we found too many operators")
      else
        throw new IllegalStateException("we lost some operators")
    }
    
    l
  }
}
