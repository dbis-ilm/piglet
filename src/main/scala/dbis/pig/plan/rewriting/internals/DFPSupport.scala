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
package dbis.pig.plan.rewriting.internals

import dbis.pig.op.{Load, PigOperator, Pipe}
import dbis.pig.plan.DataflowPlan
import org.kiama.rewriting.Rewriter._
import org.kiama.rewriting.Strategy

import scala.reflect.ClassTag

/** Provides methods for implementing some of [[dbis.pig.plan.DataflowPlan]]s methods.
 *
 */
trait DFPSupport {
  def processPlan(plan: DataflowPlan, strategy: Strategy): DataflowPlan

  def buildRemovalStrategy(rem: PigOperator): Strategy

  def fixMerge[T <: PigOperator, T2 <: PigOperator, T3 <: PigOperator](oldParent: T, oldChild: T2,
                                                                                          newParent: T3): T3

  def fixReordering[T <: PigOperator, T2 <: PigOperator](oldParent: T, newParent: T2, oldChild: T2,
                                                               newChild: T): T2

  def buildBinaryPigOperatorStrategy[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag]
    (f: (T, T2) => Option[PigOperator]): Strategy

  /** Insert `newOp` after `old` in `plan`.
    *
    * @param plan
    * @param old
    * @param newOp
    * @return
    */
  //noinspection ScalaDocMissingParameterDescription
  def insertAfter(plan: DataflowPlan, old: PigOperator, newOp: PigOperator): DataflowPlan = {
    require(!newOp.isInstanceOf[Load], "Can't insert a Load operator after another operator")
    require((old.outPipeName == "") || (old.outPipeName == newOp.inputs.head.name), 
        "The new operator has to read from the old one")
    val strategy = (op: Any) => {
      if (op == old) {
        val outIdx = old.outputs.indexWhere(_.name == newOp.inputs.head.name)
        if (outIdx > -1) {
          // newOp doesn't read from old - in practice, this can only happen if old's outPipeName is "", which means
          // it simply does not yet have any outputs
          old.outputs(outIdx).consumer = old.outputs(outIdx).consumer :+ newOp
        } else {
          old.outputs :+ List(Pipe(newOp.inputs.head.name, old, List(newOp)))
        }
        newOp.inputs.head.producer = old
        Some(op)
      }
      else {
        None
      }
    }
    processPlan(plan, manybu(strategyf(t => strategy(t))))
  }

  /** Insert `newOp` between `in` and `out` in `plan`.
    *
    * @param plan The DataflowPlan in which to insert the new operator.
    * @param inOp The Operator being the input for the new operator.
    * @param outOp The Operator being the output for the new operator.
    * @param newOp The Operator getting inserted.
    * @return The new DataflowPlan.
    */
  def insertBetween(plan: DataflowPlan, inOp: PigOperator, outOp: PigOperator, newOp: PigOperator): DataflowPlan = {
    require(!newOp.isInstanceOf[Load], "Can't insert a Load operator after another operator")
    require(inOp.outputs.flatMap(_.consumer).contains(outOp), "The input Operator must be directly connected to the output Operator")
    require(outOp.inputs.map(_.producer).contains(inOp), "The output Operator must be directly connected to the input Operator")

    //TODO:Outsource DISCONNECT, difficult since processPlan deletes Nodes with no output Pipe
    val outIdx = inOp.outputs.indexWhere(_.name == newOp.inputs.head.name)
    inOp.outputs(outIdx).consumer = inOp.outputs(outIdx).consumer.filterNot(_ == outOp)
    outOp.inputs = outOp.inputs.filterNot(_.producer == inOp)


    // Add new Operator to inOp consumers & its output Pipe to outOp inputs
    inOp.outputs(outIdx).consumer = inOp.outputs(outIdx).consumer :+ newOp
    outOp.inputs = outOp.inputs :+ newOp.outputs(outIdx)

    // Insert new Operator after inOp to Plan and append outOp
    var newPlan = plan.insertAfter(inOp, newOp)
    newPlan = newPlan.insertAfter(newOp, outOp)
    newPlan
  }
  
  /** Insert `newOp` as new path from `in` to `out` in `plan`.
    *
    * @param plan The DataflowPlan in which to insert the new operator.
    * @param inOp The Operator being the input for the new operator.
    * @param outOp The Operator being the output for the new operator.
    * @param newOp The Operator getting inserted.
    * @return The new DataflowPlan.
    */
  def insertConnect(plan: DataflowPlan, inOp: PigOperator, outOp: PigOperator, newOp: PigOperator): DataflowPlan = {
    require(!newOp.isInstanceOf[Load], "Can't insert a Load operator after another operator")
 
    val outIdx = inOp.outputs.indexWhere(_.name == newOp.inputs.head.name)
    
    // Add new Operator to inOp consumers & its output Pipe to outOp inputs
    inOp.outputs(outIdx).consumer = inOp.outputs(outIdx).consumer :+ newOp
    outOp.inputs = outOp.inputs.+:(newOp.outputs(outIdx))
    
    
    // Insert new Operator after inOp to Plan and append outOp
    var newPlan = plan.insertAfter(inOp, newOp)
    newPlan = newPlan.insertAfter(newOp, outOp)
    //outOp.inputs = outOp.inputs.tail :+ outOp.inputs.head
    newPlan
  }


  /** Replace `old` with `repl` in `plan`.
    *
    * @param plan
    * @param old
    * @param repl
    * @return A new [[dbis.pig.plan.DataflowPlan]] in which `old` has been replaced with `repl`.
    */
  //noinspection ScalaDocMissingParameterDescription
  def replace(plan: DataflowPlan, old: PigOperator, repl: PigOperator): DataflowPlan = {
    val strategy = (op: Any) => {
      if (op == old) {
        repl.inputs = old.inputs
        repl.outputs = old.outputs
        Some(repl)
      }
      else {
        None
      }
    }
    processPlan(plan, manybu(strategyf(t => strategy(t))))
  }


  /** Removes `rem` from `plan`.
    *
    * If `rem` has any child nodes in the plan, they will take its place.
    *
    * @param plan
    * @param rem
    * @param removePredecessors If true, predecessors of `rem` will be removed as well.
    * @return A new [[dbis.pig.plan.DataflowPlan]] without `rem`.
    */
  //noinspection ScalaDocMissingParameterDescription
  def remove(plan: DataflowPlan, rem: PigOperator, removePredecessors: Boolean = false): DataflowPlan = {
    var loads: List[PigOperator] = List.empty
    var newPlan: DataflowPlan = null

    if (rem.isInstanceOf[Load]) {
      newPlan = plan
      loads = loads :+ rem
    } else {
      var strat = manybu(buildRemovalStrategy(rem))

      newPlan = processPlan(plan, strat)
    }

    if (removePredecessors) {
      var nodes = Set[PigOperator]()
      var nodesToProcess = rem.inputs.map(_.producer).toSet

      while (nodesToProcess.nonEmpty) {
        val iter = nodesToProcess.iterator
        nodesToProcess = Set[PigOperator]()
        for (node <- iter) {
          nodes += node
          nodesToProcess ++= node.inputs.map(_.producer).toSet
        }
      }

      newPlan = nodes.filterNot(_.isInstanceOf[Load]).foldLeft(newPlan)((p: DataflowPlan, op: PigOperator) =>
        processPlan(p, manybu(buildRemovalStrategy(op))))
      loads = loads ++ nodes.filter(_.isInstanceOf[Load])
    }
    loads.toList.asInstanceOf[List[Load]].foreach ({ l: Load =>
      newPlan.operators = newPlan.operators.filterNot(_ == l)
    })
    newPlan
  }


  /** Swap the positions of `op1` and `op2` in `plan`
    *
    * @param plan
    * @param op1 The parent and new child operator.
    * @param op2 The child and new parent operator.
    * @return
    */
  //noinspection ScalaDocMissingParameterDescription
  def swap(plan: DataflowPlan, op1: PigOperator, op2: PigOperator): DataflowPlan = {
    val strategy = (parent: PigOperator, child: PigOperator) => {
      if (parent == op1 && child == op2) {
        val np = fixReordering(parent, child, child, parent)
        Some(np)
      }
      else {
        None
      }
    }

    processPlan(plan, manybu(buildBinaryPigOperatorStrategy(strategy)))
  }
}
