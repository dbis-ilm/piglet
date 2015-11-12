package dbis.pig.plan.rewriting.internals

import dbis.pig.plan.DataflowPlan
import dbis.pig.op.PigOperator
import dbis.pig.tools.BreadthFirstTopDownWalker

import org.kiama.rewriting.Rewriter._


import com.typesafe.scalalogging.LazyLogging
trait MergeSupport extends LazyLogging {

  
	def mergePlans(schedule: Seq[DataflowPlan]): DataflowPlan = {
	  
	  schedule.zipWithIndex.foreach { case (plan,idx) =>  
	    plan.operators.foreach { op => op.outputs.foreach { pipe => pipe.name += s"_$idx" } }  
	    
	    plan.printPlan()
	  }
	  
	  
		var mergedPlan = schedule.head

		val walker = new BreadthFirstTopDownWalker

		/* the visitor to process all operators and add them to the merged
		 * plan if necessary
		 */
		def visitor(op: PigOperator): Unit = {
	    logger.debug(s"processing op: $op")
	    
	    val mergedOps = mergedPlan.findOperator { o => o.lineageSignature == op.lineageSignature} 
	    
	    logger.debug(s"For $op --> Ops in merged plan: $mergedOps")
	    
			if(mergedOps.isEmpty) {
			  logger.debug(s"$op not already part of merged plan")
			  
			  op.inputs.map { pipe => pipe.producer.lineageSignature }
			    .flatMap { lineage => mergedPlan.findOperator { o => o.lineageSignature == lineage } }
			    .foreach { producer =>
			      
			      logger.debug(s"PRODUCER: $producer")
			      
			      // FIXME: for SPLIT and JOIN we have to find the correct pipe
			      op.inputs.head.name = producer.outPipeName
			      
  					mergedPlan = mergedPlan.insertAfter(producer, op)
  					logger.debug(s"inserted $op after $producer")
				}
			  
			} else {
			  logger.debug(s"$op already present in plan")
			}
		}

		schedule.drop(1) // skip first plan as we already copied it into mergedPlan
		  .foreach { plan => walker.walk(plan)(visitor) }  // for all remaining: visit each op and add to merged plan    

		
		println("merged plan:")
		mergedPlan.printPlan(2)
		
		// return the merged plan
		mergedPlan
	}
}