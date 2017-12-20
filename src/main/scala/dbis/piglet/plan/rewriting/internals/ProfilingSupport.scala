package dbis.piglet.plan.rewriting.internals

import dbis.piglet.op.{PigOperator, Pipe, TimingOp}
import dbis.piglet.plan.{DataflowPlan, PipeNameGenerator}
import dbis.piglet.tools.TopoSort
import dbis.piglet.tools.logging.PigletLogging

trait ProfilingSupport extends PigletLogging {
  
  def insertTimings(plan: DataflowPlan): DataflowPlan = {

    // for all operators - except consumers (STORE, DUMP) and those that were already profiled
    // FIXME: this could cause errors, because if op1 -> op2 and op1 is ignored because of this check, then op2 won't have an input timing
    for(op <- TopoSort(plan) if op.outputs.nonEmpty /*&& DataflowProfiler.getExectime(op.lineageSignature).isEmpty*/) {

      val outArr = new Array[Pipe](op.outputs.size)
      op.outputs.copyToArray(outArr)
      
      
      for(opOutPipe <- outArr) {
        val timingOp = TimingOp(Pipe(opOutPipe.name, consumers = opOutPipe.consumer), Pipe(PipeNameGenerator.generate(), producer = op), op.lineageSignature)

        ProfilingSupport.insertBetweenAll(opOutPipe, op, timingOp, plan)
      }
    }

    plan
  }
}

object ProfilingSupport {

  /**
    * Inserts the given operator between the `oldOp` and all other consumers of the given output pipe
    * @param oldPipe The output pipe of oldOp to which the new operator is insterted. All consumers of that pipe will be consumers of the new op
    * @param oldOp The old operator to insert the new op after
    * @param newOp The new operator to insert
    * @param plan The plan on which this operation is being executed
    * @return Returns the modified plan
    */
  def insertBetweenAll(oldPipe: Pipe, oldOp: PigOperator, newOp: PigOperator, plan: DataflowPlan): DataflowPlan = {

    val oldName = oldPipe.name
    val newName = PipeNameGenerator.generate()

    val newInPipe = Pipe(newName, producer = oldOp, consumers = List(newOp)) // op is producer & timing should be the only consumer

    newOp.inputs = List(newInPipe)

    val idx = oldOp.outputs.indexWhere(_.name == oldName)
    val (l1,l2) = oldOp.outputs.splitAt(idx)

    oldOp.outputs = l1 ++ List(newInPipe) ++ l2.drop(1)

    plan.operators = plan.operators :+ newOp

    plan
  }

}