/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dbis.piglet.plan.rewriting.internals

import dbis.piglet.tools.logging.PigletLogging
import dbis.piglet.op._
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.plan.rewriting.RewriterException
import dbis.piglet.tools.{BreadthFirstBottomUpWalker, BreadthFirstTopDownWalker}
import org.kiama.rewriting.Rewriter._
import org.kiama.rewriting.Strategy

import dbis.setm.SETM.timing

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** Provides methods to deal with windows in a [[dbis.piglet.plan.DataflowPlan]].
  *
  */
trait WindowSupport extends PigletLogging {
  def processPlan(newPlan: DataflowPlan, strategy: Strategy): DataflowPlan

  def processWindows(plan: DataflowPlan): DataflowPlan = timing("process windows") {
    require(plan != null, "Plan must not be null")

    var newPlan = plan

    val walker1 = new BreadthFirstTopDownWalker
    val walker2 = new BreadthFirstBottomUpWalker

    // All Window Ops: Group,Filter,Distinct,Limit,OrderBy,Foreach
    // Two modes: Group,Filter,Limit,Foreach
    // Terminator: Foreach, Join

    logger.debug(s"Searching for Window Operators")
    walker1.walk(newPlan){ op =>
      op match {
        case o: Window => {
          logger.debug(s"Found Window Operator")
          newPlan = markForWindowMode(newPlan, o)
        }
        case _ =>
      }
    }

    // Find and process Window Joins and Cross'
    val joins = ListBuffer.empty[PigOperator]
    walker2.walk(newPlan){ op =>
      op match {
        case o: Join => joins += o
        case o: Cross => joins += o
        case _ =>
      }
    }
    newPlan = processWindowJoins(newPlan, joins.toList)

    //TODO: Add Check for WindowOnly operators (distinct, orderBy, etc.)

    newPlan
  }

  def markForWindowMode(plan: DataflowPlan, windowOp: Window): DataflowPlan = {
    var lastOp: PigOperator =  new Empty(Pipe("empty"))
    val littleWalker = mutable.Queue(windowOp.outputs.flatMap(_.consumer).toSeq: _*)
    while(!littleWalker.isEmpty){
      val operator = littleWalker.dequeue()
      operator match {
        case o: Filter => {
          logger.debug(s"Rewrite Filter to WindowMode")
          o.windowMode = true
        }
        case o: Distinct => {
          logger.debug(s"Rewrite Distinct to WindowMode")
          o.windowMode = true
        }
        case o: OrderBy => {
          logger.debug(s"Rewrite OrderBy to WindowMode")
          o.windowMode = true
        }
        case o: Grouping => {
          logger.debug(s"Rewrite Grouping to WindowMode")
          o.windowMode = true
        }
        case o: Foreach => {
          logger.debug(s"Rewrite Foreach to WindowMode")
          o.windowMode = true
          //val flatten = new WindowFlatten(Pipe("flattenNode"), o.outputs.head)
          val apply = new WindowApply(Pipe(o.outPipeName+"Apply"), windowOp.outputs.head, "WindowFunc"+o.outPipeName)
          val newPlan = plan.insertConnect(windowOp, o.outputs.head.consumer.head, apply)
          apply.schema=o.schema
          return newPlan
        }
        case o: Join => {
          logger.debug(s"Found Join Node, abort")
          return plan
        }
        case o: Cross => {
          logger.debug(s"Found Cross Node, abort")
          return plan
        }
        case _ =>
      }
      littleWalker ++= operator.outputs.flatMap(_.consumer)
      if (littleWalker.isEmpty) lastOp = operator
    }
    logger.debug(s"Reached End of Plan - Adding Flatten Node")
    val before = lastOp.inputs.head
    val apply = new WindowApply(Pipe(before.name+"Apply"), windowOp.outputs.head, "WindowFunc"+before.name)
    val newPlan = plan.insertConnect(windowOp, lastOp, apply)
    apply.schema = before.producer.schema
    lastOp.schema  = before.producer.schema
    newPlan
  }


  def processWindowJoins(plan: DataflowPlan, joins: List[PigOperator]): DataflowPlan = {

    var newPlan = plan

    /*
     * Foreach Join or Cross Operator check if Input requirements are met.
     * Collect Window input relations and create new Join with Window
     * definition and window inputs as new inputs.
     */
    for(joinOp <- joins) {
      var newInputs = ListBuffer.empty[Pipe]
      var windowDef: Option[Tuple2[Int,String]] = None

      for(joinInputPipe <- joinOp.inputs){
        // Checks
        if(!joinInputPipe.producer.isInstanceOf[Window])
          throw new RewriterException("Join inputs must be Window Definitions")
        val inputWindow = joinInputPipe.producer.asInstanceOf[Window]
        if(inputWindow.window._2=="")
          throw new RewriterException("Join input windows must be defined via RANGE")
        if (!windowDef.isDefined)
          windowDef = Some(inputWindow.window)
        if(windowDef!=Some(inputWindow.window))
          throw new RewriterException("Join input windows must have the same definition")

        newInputs += inputWindow.inputs.head

        // Remove Window-Join relations
        joinOp.inputs = joinOp.inputs.filterNot(_.producer == inputWindow)
        inputWindow.outputs.foreach((out: Pipe) => {
          if(out.consumer contains joinOp)
            out.consumer = out.consumer.filterNot(_ == joinOp)
        })
      }

      val newJoin = joinOp match {
        case o: Join => Join(o.outputs.head, newInputs.toList, o.fieldExprs, windowDef.getOrElse(null.asInstanceOf[Tuple2[Int,String]]))
        case o: Cross => Cross(o.outputs.head, newInputs.toList, windowDef.getOrElse(null.asInstanceOf[Tuple2[Int,String]]))
        case _ => ???
      }

      /*
       * Replace Old Join with new Join (new Input Pipes and Window Parameter)
       */
      val strategy = (op: Any) => {
        if (op == joinOp) {
          joinOp.outputs = List.empty
          joinOp.inputs = List.empty
          Some(newJoin)
        }
        else {
          None
        }
      }
      newPlan = processPlan(newPlan, strategyf(t => strategy(t)))
    }
    newPlan
  }

}
