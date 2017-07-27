package dbis.piglet.plan.rewriting.internals

import dbis.piglet.mm.DataflowProfiler
import dbis.piglet.op.{PigOperator, Pipe, TimingOp}
import dbis.piglet.plan.{DataflowPlan, PipeNameGenerator}
import dbis.piglet.tools.TopoSort
import dbis.piglet.tools.logging.PigletLogging

trait ProfilingSupport extends PigletLogging {
  
  def insertTimings(plan: DataflowPlan): DataflowPlan = {

    // for all operators - except consumers (STORE, DUMP) and those that were already profiled
    // FIXME: this could cause errors, because if op1 -> op2 and op1 is ignored because of this check, then op2 won't have an input timing
    for(op <- TopoSort(plan) if op.outputs.nonEmpty && DataflowProfiler.getExectime(op.lineageSignature).isEmpty) {

      val outArr = new Array[Pipe](op.outputs.size)
      op.outputs.copyToArray(outArr)
      
      
      for(opOutPipe <- outArr) {
        
//        val oldName = opOutPipe.name
//        val newName = PipeNameGenerator.generate()
        
//        logger.debug(s"rewriting Pipe $opOutPipe for timing")
        
//        val timingIn = Pipe(newName, producer = op) // op is producer & timing should be the only consumer
//        val timingOut = Pipe(oldName, consumers = opOutPipe.consumer )    // timing should be only producer & op's consumer are consumers

//        timingOut.consumer.foreach{ c =>
//          val idx = c.inputs.indexWhere(_.name == oldName)
//          val (l1,l2) = c.inputs.splitAt(idx)
//          c.inputs = l1 ++ List(timingOut) ++ l2.drop(1) // the first element in l2 is the one we want to replace
//        }
        
//        val timingOp = TimingOp(
//      		timingOut,
//      		timingIn,
//      		op.lineageSignature
//    		)
//
//        timingIn.consumer = List(timingOp)
//        timingOut.producer = timingOp

//        val idx = op.outputs.indexWhere(_.name == oldName)
//        val (l1,l2) = op.outputs.splitAt(idx)
//
//        op.outputs = l1 ++ List(timingIn) ++ l2.drop(1)
        
//        logger.debug(s"adjust op outputs = ${op.outputs.mkString("\n")}")
//        logger.debug(s"op inputs are: ${op.inputs.map(_.producer).mkString("\n")}")
        
        // add timing op to list of operators
//        plan.operators = plan.operators :+ timingOp

        val timingOp = TimingOp(Pipe(opOutPipe.name, consumers = opOutPipe.consumer), Pipe(PipeNameGenerator.generate(), producer = op), op.lineageSignature)

        ProfilingSupport.insertBetweenAll(opOutPipe, op, timingOp, plan)

      }


    }

//    plan.printPlan(2)

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
//    val newOutPipe = Pipe(oldName, producer = newOp, consumers = oldPipe.consumer)    // timing should be only producer & op's consumer are consumers


//    oldPipe.consumer.foreach{ c =>
//
//      val idx = c.inputs.indexWhere(_.name == oldName)
//      val (l1,l2) = c.inputs.splitAt(idx)
//      c.inputs = l1 ++ List(newOutPipe) ++ l2.drop(1) // the first element in l2 is the one we want to replace
//
//    }

//    newOp.outputs = List(newOutPipe)
//    val idx0 = newOp.outputs.indexWhere(_.name == oldName)
//    val (l10,l20) = newOp.outputs.splitAt(idx0)
//    newOp.outputs = l10 ++ List(newOutPipe) ++ l20.drop(1)



      newOp.inputs = List(newInPipe)
//
//    newInPipe.consumer = List(newOp)
//    newOutPipe.producer = newOp

    val idx = oldOp.outputs.indexWhere(_.name == oldName)
    val (l1,l2) = oldOp.outputs.splitAt(idx)

    oldOp.outputs = l1 ++ List(newInPipe) ++ l2.drop(1)

    plan.operators = plan.operators :+ newOp

    plan
  }

}