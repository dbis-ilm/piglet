package dbis.pig.plan.rewriting.internals

import dbis.pig.plan.DataflowPlan
import dbis.pig.op.PigOperator
import dbis.pig.tools.BreadthFirstTopDownWalker
import org.kiama.rewriting.Rewriter._
import dbis.pig.op.Join
import dbis.pig.op.Cross
import dbis.pig.op.Load
import dbis.pig.tools.logging.PigletLogging

trait MergeSupport extends PigletLogging {

  
	def mergePlans(schedule: Seq[DataflowPlan]): DataflowPlan = {
	  
	  schedule.zipWithIndex.foreach { case (plan,idx) =>  
	    plan.operators.foreach { op => op.outputs.foreach { pipe => pipe.name += s"_$idx" } }  
	    
	    plan.printPlan()
	  }
	  
	  
		var mergedPlan = schedule.head

		val walker = new BreadthFirstTopDownWalker

		val deferrPlanConstruction = false
		var needPlanConstruction = false
		
		/* the visitor to process all operators and add them to the merged
		 * plan if necessary
		 */
		def visitor(op: PigOperator): Unit = {
	    logger.debug(s"processing op: $op")
	    
	    val mergedOps = mergedPlan.findOperator { o => o.lineageSignature == op.lineageSignature} 
	    
	    logger.debug(s"For $op --> Ops in merged plan: $mergedOps")
	    
			if(mergedOps.isEmpty) {
			  logger.debug(s"$op not already part of merged plan")
			  
			  op match {
			    
			    case Load(_,_,_,_,_) => 
			      mergedPlan.addOperator(List(op), deferrPlanConstruction)
			      needPlanConstruction = true
			    case Join(_,_,_,_) | Cross(_,_,_) => {
			      
			      op.inputs.foreach { pipe =>
			        val prod = mergedPlan.findOperator { o => o.lineageSignature == pipe.producer.lineageSignature }.headOption
			        if(prod.isDefined)
			          pipe.name = prod.get.outPipeName
			        else
			          logger.warn("No producer found for $op -- this shouldn't happen?!")
			      }
			      
			      mergedPlan.addOperator(List(op), deferrPlanConstruction)
			      needPlanConstruction = true
			    } 
			    case _ =>
			      
			      op.inputs // since join and cross are handled separately, inputs is 0 (LOAD) or 1
    			    .flatMap { pipe => mergedPlan.findOperator { o => o.lineageSignature == pipe.producer.lineageSignature } } // get the producer 
  			      .foreach { producer => // process the single producer
    			      
    			      op.inputs.head.name = producer.outPipeName
    			      
      					mergedPlan = mergedPlan.insertAfter(producer, op)
      					logger.debug(s"inserted $op after $producer")
    				}
			  }
			} else {
			  logger.debug(s"$op already present in plan")
			}
		}

		schedule.drop(1) // skip first plan as we already copied it into mergedPlan
		  .foreach { plan => walker.walk(plan)(visitor) }  // for all remaining: visit each op and add to merged plan    

		if(needPlanConstruction && deferrPlanConstruction) 
			mergedPlan.constructPlan(mergedPlan.operators)
		
		println("merged plan:")
		mergedPlan.printPlan(2)
		
		// return the merged plan
		mergedPlan
	}
}