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

import dbis.pig.op.{Empty, PigOperator, Pipe}
import org.kiama.rewriting.Rewriter._
import org.kiama.rewriting.Strategy

import scala.reflect.{ClassTag, classTag}

/** Provides methods for building [[org.kiama.rewriting.Strategy]] objects.
  *
  */
trait StrategyBuilders {
  def fixReplacement[T <: PigOperator](old: PigOperator) (new_ : T): T

  /** Returns a strategy to remove `rem` from a DataflowPlan
    *
    * @param rem
    * @return
    */
  //noinspection ScalaDocMissingParameterDescription
  def buildRemovalStrategy(rem: PigOperator): Strategy = {
    strategyf((op: Any) => {
      if (op == rem) {
        val pigOp = op.asInstanceOf[PigOperator]
        if (pigOp.inputs.isEmpty) {
          val consumers = pigOp.outputs.flatMap(_.consumer)
          if (consumers.isEmpty) {
            Some(Empty(Pipe("")))
          }
          else {
            consumers foreach (_.inputs = List.empty)
            Some(consumers.toList)
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
          Some(newOps)
        }
      }
      else {
        None
      }
    })}

  /** Builds the strategy for [[dbis.pig.plan.rewriting.Rewriter.addOperatorReplacementStrategy]].
    *
    * @param f
    * @return
    */
  def buildOperatorReplacementStrategy[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag]
   (f: T => Option[T2]): Strategy = {

    def inner(term: T): Option[T2] = {
      f(term) map (fixReplacement[T2](term))
    }

    val wrapper = buildTypedCaseWrapper[T, T2](inner)
    strategyf(t => wrapper(t))
  }

  def buildBinaryPigOperatorStrategy[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag]
  (f: (T, T2) => Option[PigOperator]): Strategy = {
    strategyf(op => {
      op match {
        case `op` if classTag[T].runtimeClass.isInstance(op) =>
          val parent = op.asInstanceOf[T]
          if (parent.outputs.length == 1 && parent.outputs.head.consumer.length == 1) {
            val op2 = parent.outputs.head.consumer.head
            op2 match {
              case `op2` if classTag[T2].runtimeClass.isInstance(op2) && op2.inputs.length == 1 =>
                val child = op2.asInstanceOf[T2]
                f(parent, child)
              case _ => None
            }
          }
          else {
            None
          }
        case _ => None
      }
    })
  }

  /** Given a function `f: (T => Option[T])`, return a function that applies `f` if the input term is of type `T`.
    *
    * @param f
    * @tparam T
    * @return
    */
  def buildTypedCaseWrapper[T <: PigOperator : ClassTag, T2](f: (T => Option[T2])): (Any => Option[T2]) = {
    val wrapper = {term: Any => term match {
      case _ if classTag[T].runtimeClass.isInstance(term) => f(term.asInstanceOf[T])
      case _ => None
    }}
    wrapper
  }
}
