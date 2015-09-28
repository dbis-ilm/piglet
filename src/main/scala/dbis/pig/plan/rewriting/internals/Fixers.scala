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

import dbis.pig.op.{PigOperator, Pipe}

/** Provides helper methods for fixing inputs and outputs after rewriting operations.
  *
  */
trait Fixers {
  /** Fix the inputs and outputs attributes of PigOperators after an operation merged two of them into one.
    *
    * @param oldParent The old parent operator.
    * @param oldChild The old child operator.
    * @param newParent The new operator.
    * @tparam T The type of the old parent operator.
    * @tparam T2 The type of the old child operator.
    * @tparam T3 The type of the new operator.
    * @return
    */
  def fixInputsAndOutputs[T <: PigOperator, T2 <: PigOperator, T3 <: PigOperator](oldParent: T, oldChild: T2,
                                                                                  newParent: T3): T3 = {
    newParent.inputs = oldParent.inputs
    newParent.outputs = oldChild.outputs

    // Each Operator that has oldChild in its inputs list as a producer needs to have it replaced with newParent
    oldChild.outputs foreach { output =>
      output.consumer foreach { op =>
        op.inputs = op.inputs.filter(_.producer != oldChild) :+ Pipe(newParent.outPipeName, newParent, List(op))
      }
    }

    // Replacing oldParent with newParent in the outputs attribute of oldParents inputs producers is done by kiamas
    // Rewritable trait
    newParent
  }

  /** Fix the inputs and outputs attributes of PigOperators after two of them have been reordered.
    *
    * @param oldParent The old parent operator.
    * @param newParent The new parent operator.
    * @param oldChild The old child operator.
    * @param newChild The new child Operator.
    * @tparam T The type of the old parent and new child operators.
    * @tparam T2 The type of the old child and new parent operators.
    * @return
    */
  def fixInputsAndOutputs[T <: PigOperator, T2 <: PigOperator](oldParent: T, newParent: T2, oldChild: T2,
                                                               newChild: T): T2 = {
    // If oldParent == newChild (for example when this is called from `swap`, we need to save oldParent.outPipename
    // because it depends on oldParent.outputs
    val oldparent_outpipename = oldParent.outPipeName

    // See above, of oldParent == newChild, we need to use oldParent.inputs while we can
    newParent.inputs = oldParent.inputs

    newChild.inputs = List(Pipe(newParent.outPipeName, newParent, List(newChild)))
    newChild.outputs = oldChild.outputs

    newParent.outputs = List(Pipe(oldparent_outpipename, newParent, List(newChild)))

    // Each Operator that has oldChild in its inputs list as a producer needs to have it replaced with newChild
    oldChild.outputs foreach { output =>
      output.consumer foreach { op =>
        op.inputs = op.inputs.filter(_.producer != oldChild) :+ Pipe(newParent.outPipeName, newChild, List(op))
      }
    }

    // Replacing oldParent with newParent in oldParents inputs outputs list is done by kiamas Rewritable trait
    newParent
  }

  /** Changes inputs and outputs so that ``toBePulled`` is between ``multipleInputOp`` and ``indicator``.
    *
    * ``toBePulled`` has to be a consumer of ``multipleInputOp``s output pipes.
    * @param toBePulled
    * @param multipleInputOp
    * @param indicator
    * @return
    */
  def pullOpAcrossMultipleInputOp(toBePulled: PigOperator, multipleInputOp: PigOperator, indicator: PigOperator):
  PigOperator = {
    require(multipleInputOp.outputs.flatMap(_.consumer) contains toBePulled, "toBePulled is not a consumer of " +
      "multipleInputOp")
    // First, make the toBePulled a consumer of the correct input
    indicator.outputs foreach { outp =>
      if (outp.consumer contains multipleInputOp) {
        outp.consumer = outp.consumer.filterNot(_ == multipleInputOp) :+ toBePulled
      }
    }

    toBePulled.inputs = toBePulled.inputs.filterNot(_.producer == multipleInputOp) :+ Pipe(indicator.outPipeName,
      indicator)

    // Second, make the toBePulled an input of the multipleInputOp
    multipleInputOp.inputs = multipleInputOp.inputs.filterNot(_.name == indicator.outPipeName) :+ Pipe(toBePulled.outPipeName, toBePulled)

    val oldOutPipeName = multipleInputOp.outPipeName

    // Third, replace the toBePulled in the multipleInputOps outputs with the Filters outputs
    multipleInputOp.outputs foreach { outp =>
      if (outp.consumer contains toBePulled) {
        outp.consumer = outp.consumer.filterNot(_ == toBePulled) ++ toBePulled.outputs.flatMap(_.consumer)
      }
    }

    // Fourth, make the multipleInputOp the producer of all the toBePulleds outputs inputs
    toBePulled.outputs foreach { outp =>
      outp.consumer foreach { cons =>
        cons.inputs = cons.inputs map { cinp =>
          if (cinp.producer == toBePulled) {
            Pipe(oldOutPipeName, multipleInputOp)
          } else {
            cinp
          }
        }
      }
    }

    // Fifth, make multipleInputOp the consumer of all of toBePulled output pipes
    toBePulled.outputs foreach { outp =>
      outp.consumer = List(multipleInputOp)
    }

    toBePulled
  }
}
