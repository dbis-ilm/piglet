package dbis.piglet.plan.rewriting.internals

import dbis.piglet.plan.DataflowPlan
import dbis.piglet.plan.PipeNameGenerator
import dbis.piglet.op.{TimingOp, Pipe}
import dbis.piglet.op.PigOperator
import dbis.piglet.tools.TopoSort
import dbis.piglet.plan.InvalidPlanException
import dbis.piglet.tools.logging.PigletLogging
import scala.collection.mutable.ListBuffer

trait ProfilingSupport extends PigletLogging {
  
  def insertTimings(plan: DataflowPlan): DataflowPlan = {

    
    for(op <- TopoSort.sort(plan) if op.outputs.nonEmpty) {
      
      val outArr = new Array[Pipe](op.outputs.size)
      op.outputs.copyToArray(outArr)
      
      
      for(opOutPipe <- outArr) {
        
        val oldName = opOutPipe.name
        val newName = PipeNameGenerator.generate()
        
        logger.debug(s"rewriting Pipe $opOutPipe for timing")
        
        val tIn = Pipe(newName, producer = op) // op is producer & timing should be the only consumer
        val tOut = Pipe(oldName, consumers = opOutPipe.consumer )    // timing should be only producer & op's consumer are consumers

        tOut.consumer.foreach{ c =>
          val idx = c.inputs.indexWhere(_.name == oldName)
          val (l1,l2) = c.inputs.splitAt(idx)
          c.inputs = l1 ++ List(tOut) ++ l2.drop(1) // the first element in l2 is the one we want to replace
        }
        
        val timingOp = TimingOp(
      		tOut,
      		tIn,
      		op.lineageSignature
    		)

        tIn.consumer = List(timingOp)
        tOut.producer = timingOp

        val idx = op.outputs.indexWhere(_.name == oldName)
        val (l1,l2) = op.outputs.splitAt(idx)
        
        op.outputs = l1 ++ List(tIn) ++ l2.drop(1) 
        
//        logger.debug(s"adjust op outputs = ${op.outputs.mkString("\n")}")
//        logger.debug(s"op inputs are: ${op.inputs.map(_.producer).mkString("\n")}")
        
        // add timing op to list of operators
        plan.operators = plan.operators :+ timingOp
      }
      
      
    }
    
//    plan.printPlan(2)
    
    plan
  } 
}
