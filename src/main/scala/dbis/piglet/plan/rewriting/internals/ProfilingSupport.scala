package dbis.piglet.plan.rewriting.internals

import dbis.piglet.plan.DataflowPlan
import dbis.piglet.plan.PipeNameGenerator
import dbis.piglet.op.{TimingOp, Pipe}
import dbis.piglet.op.PigOperator
import dbis.piglet.tools.TopoSort

trait ProfilingSupport {
  
  def insertTimings(plan: DataflowPlan): DataflowPlan = {

    for(op <- TopoSort.sort(plan) if op.outputs.nonEmpty) {
      for(opOutPipe <- op.outputs) {
        
        val oldName = opOutPipe.name
        val newName = PipeNameGenerator.generate()
        
        val tIn = Pipe(newName, op) // op is producer  timing should be only consumer
        val tOut = Pipe(oldName, consumers = opOutPipe.consumer )    // timing should be only producer op's consumer are consumers
        
        val timingOp = TimingOp(
          tOut,
          tIn,
          op.lineageSignature
        )
        
        
        timingOp.inputs = List(tIn)   // this sets timingOp as consumer in the pipes 
        timingOp.outputs = List(tOut) // this sets timingOp as producer
        
        op.outputs = List(tIn)            
        
        plan.operators = plan.operators :+ timingOp
      }
    }
    plan
  } 
}
