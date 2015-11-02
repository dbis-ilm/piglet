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
  /** Makes ``succ`` a successor of ``pred``.
    *
    */
  @throws[IllegalArgumentException]("If pred does not have exactly one output pipe")
  @throws[IllegalArgumentException]("If succ has more than one input pipe")
  @throws[IllegalArgumentException]("If succ already has an input pipe with a producer that's not pred")
  def connect(pred: PigOperator, succ: PigOperator): Unit = {
    require(pred.outputs.length == 1, "The new predecessor does not have exactly one output pipe")
    require(succ.inputs.length < 2, "The new successor has more than one input pipe")
    require(succ.inputs.length == 0 || succ.inputs.head.producer == null || succ.inputs.head.producer == pred,
      "The new successors input pipe already has a producer that is not the same as pred")
    require(succ.inputs.length == 0 || pred.outputs.find(_.name == succ.inPipeName).isDefined,
      pred + " writes " + pred.outPipeName + ", but " + succ + " reads " + succ.inPipeName)

    // If there is an input pipe matching `pred`s output name, use it, otherwise build a new one
    val inPipe = succ.inputs.find {_.name == pred.outputs.head.name}.orElse(Some(Pipe(pred.outputs.head.name, pred))).get
    inPipe.consumer = List(succ)
    succ.inputs = List(inPipe)

    pred.outputs.head.consumer = pred.outputs.head.consumer :+ succ
  }

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
  def fixMerge[T <: PigOperator, T2 <: PigOperator, T3 <: PigOperator](oldParent: T, oldChild: T2,
                                                                                  newParent: T3): T3 = {
    newParent.inputs = oldParent.inputs
    newParent.outputs = oldChild.outputs

    replaceOpInSuccessorsInputs(oldChild, newParent)

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
  def fixReordering[T <: PigOperator, T2 <: PigOperator](oldParent: T, newParent: T2, oldChild: T2,
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
    // Put the new pipe to toBePulled at the same position as the old one to indicator so the position of fields
    // still matches
    val index = multipleInputOp.inputs.indexWhere(_.name == indicator.outPipeName)
    multipleInputOp.inputs = multipleInputOp.inputs.updated(index, Pipe(toBePulled.outPipeName, toBePulled))

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

    // Now that toBePulled doesn't read from multipleInputOp anymore, its schema needs an update.
    toBePulled.constructSchema

    toBePulled
  }

  /** Changes inputs and outputs such that `newParent` is the new consumer of `old`s inputs outputs and newChild the
    * producer of `old`s outputs inputs.
    *
    * `newChild` does not have do be a direct successor of `newParent`.
    *
    * @param old
    * @param newParent
    * @param newChild
    * @tparam T
    * @tparam T2
    * @tparam T3
    * @return
    */
  def fixReplacementwithMultipleOperators[T <: PigOperator, T2 <: PigOperator, T3 <: PigOperator]
  (old: T, newParent: T2, newChild: T3): T2 = {
    newParent.inputs = old.inputs
    newChild.outputs = old.outputs

    // Remove `old` as a consumer of its inputs
    old.inputs.foreach { in =>
     in.removeConsumer(old)
    }

    replaceOpInSuccessorsInputs(old, newChild)

    newParent
  }

  /** Replaces ``oldOp`` in its outputs inputs as a producer with ``newOp``.
    *
    * @param oldOp
    * @param newOp
    */
  def replaceOpInSuccessorsInputs(oldOp: PigOperator, newOp: PigOperator) = {
    oldOp.outputs foreach { output =>
      output.consumer foreach { consumer =>
        consumer.inputs foreach { input =>
          // If `op` (the old term) is the producer of any of the input pipes of `newChild` (the new terms)
          // successors, replace it with `newChild` in that attribute. Replacing `op` with `other_filter` in
          // the pipes on `newChild` itself is not necessary because the setters of `inputs` and `outputs` do
          // that.
          if (input.producer == oldOp) {
            input.producer = newOp
          }
        }
      }
    }
  }

  /** Changes inputs and outputs after ``old`` has been replaced by ``new_``.
    *
    * @param old
    * @param new_
    * @tparam T
    * @return ``new_``
    */
  def fixReplacement[T <: PigOperator](old: PigOperator) (new_ : T): T = {
    new_.outputs foreach { output =>
      output.consumer foreach { consumer =>
        consumer.inputs foreach { input =>
          // If `t` (the old term) is the producer of any of the input pipes of `op` (the new terms) successors,
          // replace it with `op` in that attribute. Replacing `t` with `op` in the pipes on `op` itself is not
          // necessary because the setters of `inputs` and `outputs` do that.
          if (input.producer == old) {
            input.producer = new_
          }
        }
      }
    }
    new_
  }
}
