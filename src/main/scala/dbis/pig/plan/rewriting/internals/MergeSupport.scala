package dbis.pig.plan.rewriting.internals

import dbis.pig.plan.DataflowPlan
import dbis.pig.tools.DepthFirstTopDownWalker
import dbis.pig.op.PigOperator
import com.typesafe.scalalogging.LazyLogging
import org.kiama.rewriting.Strategy
import dbis.pig.plan.DataflowPlan
import dbis.pig.plan.DataflowPlan
import dbis.pig.op.PigOperator
import org.kiama.rewriting.Rewriter._

trait MergeSupport extends LazyLogging {

//  def processPlan(newPlan: DataflowPlan, strategy: Strategy): DataflowPlan
//  
//  def mergePlans2(schedule: Seq[DataflowPlan]): DataflowPlan = {
//    
//    var mergedPlan = schedule.head
//    
//    val strategy = (op: Any) => {
//      val opOp = op.asInstanceOf[PigOperator]
//      if(!mergedPlan.containsOperator(opOp)) {
//        opOp.inputs.map { p => p.producer }.foreach { prod => mergedPlan.insertAfter(prod, opOp) }
//        Some(opOp)
//      } else
//        None
//    }
//    
//    // TODO: how to process multiple plans here?
//    processPlan(mergedPlan, strategyf(t => strategy(t)))
//  }
  
	def mergePlans(schedule: Seq[DataflowPlan]): DataflowPlan = {
		var mergedPlan = schedule.head

		val walker = new DepthFirstTopDownWalker

		/* the visitor to process all operators and add them to the merged
		 * plan if necessary
		 */
		def visitor(op: PigOperator): Unit = {
	    logger.debug(s"processing op: $op")
			if(!mergedPlan.containsOperator(op)) {
			  logger.debug(s"$op not already part of merged plan")
			  
			  op.inputs.map { pipe => pipe.producer.lineageSignature }
			    .flatMap { lineage => mergedPlan.findOperator { o => o.lineageSignature == lineage } }
			    .foreach { producer =>
  					mergedPlan = mergedPlan.insertAfter(producer, op)
  					logger.debug(s"inserted $op after $producer")
				}
			  
//				op.inputs.map { pipe => pipe.producer }.foreach { producer =>
//					mergedPlan = mergedPlan.insertAfter(producer, op)
//					logger.debug(s"inserted $op after $producer")
//				}
			} else {
			  logger.debug(s"$op already present in plan")
			}
		}

		schedule.drop(1) // skip first plan as we already copied it into mergedPlan
		  .foreach { plan => walker.walk(plan)(visitor) }  // for all remaining: visit each op and add to merged plan    

		
//		println("merged plan:")
//		mergedPlan.printPlan(2)
		
		// return the merged plan
		mergedPlan
	}
}