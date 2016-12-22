package dbis.piglet.plan.rewriting.internals

import dbis.piglet.plan.DataflowPlan
import dbis.piglet.op.{TimingOp, Pipe}
import dbis.piglet.op.PigOperator
import dbis.piglet.plan.PipeNameGenerator
import java.net.URI

trait ProfilingSupport {
  
  def insertTimings(plan: DataflowPlan, url: URI): DataflowPlan = {
    var newPlan = plan
    for(op <- plan.operators) {
      
      op.outputs.flatMap(_.consumer).foreach { consumer => 
        val timingOp = TimingOp(
          Pipe(PipeNameGenerator.generate()),
          Pipe(op.outPipeName),
          op.lineageString)
    	  newPlan = newPlan.insertBetween(op, consumer, timingOp)
      }
      
      
      
//      val timingOp = TimingOp(
//          Pipe(PipeNameGenerator.generate()),
//          Pipe(op.outPipeName),
//          op.lineageString)
//    	newPlan = newPlan.insertAfter(op, timingOp)
    }
    newPlan  
  } 
}