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

import dbis.pig.op.{Pipe, Empty, PigOperator}
import org.kiama.rewriting.Strategy

object Functions {
  def merge[T <: PigOperator, T2 <: PigOperator, T3 <: PigOperator]
  (term1: T, term2: T2, f: (T, T2) => T3): T3 = {
    val newOp = f(term1, term2)
    Rewriter.fixInputsAndOutputs(term1, term2, newOp)
  }

  def replace[T <: PigOperator, T2 <: PigOperator](old: T, new_ : T2): T2 =
    Rewriter.fixReplacement(old) (new_)

  /** Cut off ``op`` from the data flow. If it has any child nodes, they will take its place. If it doesn't, an
    * [[dbis.pig.op.Empty]] operator will be inserted instead.
    *
    * @param op
    * @return
    */
  def remove(op: PigOperator): Any = {
    val pigOp = op.asInstanceOf[PigOperator]
    if (pigOp.inputs.isEmpty) {
      val consumers = pigOp.outputs.flatMap(_.consumer)
      if (consumers.isEmpty) {
        Empty(Pipe(""))
      }
      else {
        consumers foreach (_.inputs = List.empty)
        consumers.toList
      }
    }
    else {
      val newOps = pigOp.outputs.flatMap(_.consumer).map((inOp: PigOperator) => {
        // Remove input pipes to `op` and replace them with `ops` input pipes
        inOp.inputs = inOp.inputs.filterNot(_.producer == pigOp) ++ pigOp.inputs
        inOp
      })
      // Replace `op` in its inputs output pipes with `ops` children
      pigOp.inputs.map(_.producer).foreach(_.outputs.foreach((out: Pipe) => {
        if (out.consumer contains op) {
          out.consumer = out.consumer.filterNot(_ == op) ++ newOps
        }
      }))
      newOps
    }
  }

  def swap[T <: PigOperator, T2 <: PigOperator](parent: T, child: T2): T2 =
    Rewriter.fixInputsAndOutputs(parent, child, child, parent)
}
