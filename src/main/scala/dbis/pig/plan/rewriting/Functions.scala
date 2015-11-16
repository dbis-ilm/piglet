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
package dbis.pig.plan.rewriting

import dbis.pig.op.{Empty, PigOperator, Pipe}
import dbis.pig.plan.PipeNameGenerator

object Functions {
  /** Merge ``term1`` and ``term2`` into a new term by applying them to ``f``.
    *
    * @param term1
    * @param term2
    * @param f
    * @tparam T
    * @tparam T2
    * @tparam T3
    * @return
    */
  def merge[T <: PigOperator, T2 <: PigOperator, T3 <: PigOperator]
  (term1: T, term2: T2, f: (T, T2) => T3): T3 = {
    val newOp = f(term1, term2)
    Rewriter.fixMerge(term1, term2, newOp)
  }

  /** Set the pipes of ``ops`` such that the data flows from the first element of ``ops`` to the last.
    *
    * @param ops
    * @return
    */
  def newFlow(ops: PigOperator*) = {
    require(ops.length >= 2)
    ops.reduceLeft { (op1: PigOperator, op2: PigOperator) =>
      Rewriter.connect(op1, op2)
      op2
    }
    ops.head
  }

  /** Set the pipes of ``ops`` such that the data flows from the first element of ``ops`` to the last, removing
    * existing pipes in the process.
    *
    * @param ops
    * @return
    */
  def newFlowIgnoringOld(ops: PigOperator*) = {
    require(ops.length >= 2)
    ops.reduceLeft { (op1: PigOperator, op2: PigOperator) =>
      op1.outputs = List(Pipe(PipeNameGenerator.generate()))
      op2.inputs = List.empty
      Rewriter.connect(op1, op2)
      op2
    }
    ops.head
  }

  /** Replace ``old`` with ``new_``.
    *
    * @param old
    * @param new_
    * @tparam T
    * @tparam T2
    * @return
    */
  def replace[T <: PigOperator, T2 <: PigOperator](old: T, new_ : T2): T2 =
    Rewriter.fixReplacement(old) (new_)

  /** Cut off ``op`` from the data flow. If it has any child nodes, they will take its place. If it doesn't, an
    * [[dbis.pig.op.Empty]] operator will be inserted instead.
    *
    * @param op
    * @return
    */
  def remove(op: PigOperator): Any = {
    if (op.inputs.isEmpty) {
      val consumers = op.outputs.flatMap(_.consumer)
      if (consumers.isEmpty) {
        Empty(Pipe(""))
      }
      else {
        consumers foreach (_.inputs = List.empty)
        consumers.toList
      }
    }
    else {
      val newOps = op.outputs.flatMap(_.consumer).map((inOp: PigOperator) => {
        // Remove input pipes to `op` and replace them with `ops` input pipes
        inOp.inputs = inOp.inputs.filterNot(_.producer == op) ++ op.inputs
        inOp
      })
      // Replace `op` in its inputs output pipes with `ops` children
      op.inputs.map(_.producer).foreach(_.outputs.foreach((out: Pipe) => {
        if (out.consumer contains op) {
          out.consumer = out.consumer.filterNot(_ == op) ++ newOps
        }
      }))
      newOps
    }
  }

  /** Swap parent and child.
    *
    * @param parent
    * @param child
    * @tparam T
    * @tparam T2
    * @return
    */
  def swap[T <: PigOperator, T2 <: PigOperator](parent: T, child: T2): T2 =
    Rewriter.fixReordering(parent, child, child, parent)
}
