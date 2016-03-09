package dbis.pig.plan

import dbis.pig.op.{PigOperator,Join,Cross,Load}
import dbis.pig.tools.logging.PigletLogging
import dbis.pig.tools.BreadthFirstTopDownWalker
import dbis.setm.SETM.timing

object PlanMerger extends PigletLogging {

  
	def mergePlans(schedule: Seq[DataflowPlan]): DataflowPlan = timing("merge plans") {
	  
	  val indexedSchedule = schedule.zipWithIndex
	  
	  indexedSchedule.foreach { case (plan,idx) =>  
	    plan.operators.foreach { _.outputs.foreach { pipe => pipe.name += s"_$idx" } }    
	  }
	  
	  // start with the first plan as basis
		var mergedPlan = schedule.head

		val walker = new BreadthFirstTopDownWalker
		
		// just to avoid magic "numbers" in later code
		val deferrPlanConstruction = true
		
		/* need to construct if load or join/cross was found
		 * because they will be added newly to the list of operators
		 * from which the plan will be created
		 */
		var needPlanConstruction = false
		
		/* the visitor to process all operators and add them to the merged
		 * plan if necessary
		 */
		def visitor(op: PigOperator): Unit = {
		  logger.debug("")
	    logger.debug(s"processing op: $op")
	  
	    // try to find the current op in merged plan
	    val mergedOps = mergedPlan.findOperator { o => o.lineageSignature == op.lineageSignature} 
	    
	    logger.debug(s"For $op --> Ops in merged plan: $mergedOps")
	    
	    // not found --> add it
			if(mergedOps.isEmpty) {
			  logger.debug(s"$op not already part of merged plan")
			  
			  // some ops need special treatment
			  op match {
			    
			    // for load we don't need to adjust inputs
			    case Load(_,_,_,_,_) => 
			      mergedPlan.addOperator(List(op), deferrPlanConstruction)
			      needPlanConstruction = true
			      
			    /* for join and cross we need to find the correct input operators in the merged plan
			     * since we use BFS, we should always find all inputs as they were processed before
			     * the actual join/cross op
			     */ 
			    case Join(_,_,_,_) | Cross(_,_,_) => {
			      
			      op.inputs.foreach { pipe =>
			        val prod = mergedPlan.findOperator { o => o.lineageSignature == pipe.producer.lineageSignature }.headOption
			        if(prod.isDefined)
			          pipe.name = prod.get.outPipeName
			        else
			          logger.warn(s"No producer found for $op -- this shouldn't happen?!")
			      }
			      
			      // add op to the list but do not create plan now 
			      logger.debug(s"try to add $op with deferr=$deferrPlanConstruction")
			      mergedPlan.addOperator(List(op), deferrPlanConstruction)
			      needPlanConstruction = true
			    } 
			    case _ =>
			      
			      op.inputs // since join and cross are handled separately, inputs is  1
    			    .flatMap { pipe => mergedPlan.findOperator { o => o.lineageSignature == pipe.producer.lineageSignature } } // get the producer 
  			      .foreach { producer => // process the single producer
    			      
    			      op.inputs.head.name = producer.outPipeName
    			      
//      					mergedPlan = mergedPlan.insertAfter(producer, op)
    			      mergedPlan.addOperator(List(op), deferrPlanConstruction)
      					logger.debug(s"inserted $op after $producer")
    				}
			  }
			} else {
			  logger.debug(s"$op already present in plan")
			}
		}

		indexedSchedule.drop(1) // skip first plan as we already copied it into mergedPlan
		  .foreach {  case (plan,idx) => 
		    // for all remaining: visit each op and add to merged plan 
		    logger.debug(s"processing plan no. #$idx")
		    walker.walk(plan)(visitor) 
	    }      

		
		// we need to newly construct the plan if we added ops to the list of operators
		// but did not construct the plan yet (load and join/cross)
		if(needPlanConstruction && deferrPlanConstruction) 
			mergedPlan.constructPlan(mergedPlan.operators)
		
//		println("merged plan:")
//		mergedPlan.printPlan(2)
		
		// return the merged plan
		mergedPlan
	}
}