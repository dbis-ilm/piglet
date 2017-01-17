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
          c.inputs = c.inputs.filterNot(_.name == oldName) :+ tOut
        }
        
        val timingOp = TimingOp(
      		tOut,
      		tIn,
      		op.lineageSignature
    		)

        tIn.consumer = List(timingOp)
        tOut.producer = timingOp
        
        op.outputs = op.outputs.filterNot(_.name == oldName) :+ tIn
        
//        logger.debug(s"adjust op outputs = ${op.outputs.mkString("\n")}")
//        logger.debug(s"op inputs are: ${op.inputs.map(_.producer).mkString("\n")}")
        
        // add timing op to list of operators
        plan.operators = plan.operators :+ timingOp
        
//        op.constructSchema
      }
    }
    
//    plan.printPlan(2)
    
    plan
  } 
}
