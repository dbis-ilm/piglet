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

import dbis.pig.op._
import org.kiama.rewriting.Strategy

import scala.reflect.ClassTag

/** Provides methods for quickly adding simple strategies.
  *
  */
trait FastStrategyAdder {
  def fixInputsAndOutputs[T <: PigOperator, T2 <: PigOperator, T3 <: PigOperator](oldParent: T, oldChild: T2,
                                                                                  newParent: T3): T3
  def buildBinaryPigOperatorStrategy[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag]
  (f: (T, T2) => Option[PigOperator]): Strategy
  def addStrategy(strategy: Strategy): Unit

  def fixInputsAndOutputs[T <: PigOperator, T2 <: PigOperator](oldParent: T, newParent: T2, oldChild: T2,
                                                               newChild: T): T2

  def buildTypedCaseWrapper[T <: PigOperator : ClassTag, T2](f: (T => Option[T2])): (Any => Option[T2])

  def addStrategy(f: Any => Option[PigOperator]): Unit

  /** Add a new strategy for merging operators of two types.
    *
    * An example method to merge Filter operators is
    * {{{
    *  def mergeFilters(parent: Filter, child: Filter): Option[PigOperator] = {
    *    Some(Filter(child.output.get, parent.initialInPipeName, And(parent.pred, child.pred)))
    *  }
    * }}}
    *
    * It can be added to the rewriter via
    * {{{
    *  merge[Filter, Filter](mergeFilters)
    * }}}
    *
    * @param f The function to perform the merge. It does not have to modify inputs and outputs, this will be done
    *          automatically.
    * @tparam T The type of the first operator.
    * @tparam T2 The type of the second operator.
    */
  def merge[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag](f: (T, T2) => Option[PigOperator]):
  Unit = {
    val strategy = (parent: T, child: T2) =>
      f(parent, child).map(fixInputsAndOutputs(parent, child, _))
    addBinaryPigOperatorStrategy(strategy)
  }

  /** Add a new strategy for reordering two operators.
    *
    * An additional function `f` can be supplied that performs the reordering. This is useful if the reordering can
    * only be performed in some cases that can't be expressed by just the types.
    *
    * A new reordering strategy can be added to the rewriter via
    * {{{
    *  reorder[OrderBy, Filter](f _)
    * }}}
    *
    * @param f
    * @tparam T The type of the parent operator.
    * @tparam T2 The type of the child operator.
    */
  //noinspection ScalaDocMissingParameterDescription
  def reorder[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag](f: (T, T2) => Option[(T2, T)]):
  Unit = {
    val strategy = (parent: T, child: T2) =>
      f(parent, child).map(tup => fixInputsAndOutputs(tup._2, tup._1, tup._1, tup._2))
    addBinaryPigOperatorStrategy(strategy)
  }

  /** Add a new strategy for reordering two operators.
    *
    * A new reordering strategy can be added to the rewriter via
    * {{{
    *  reorder[OrderBy, Filter]
    * }}}
    *
    * @tparam T The type of the parent operator.
    * @tparam T2 The type of the child operator.
    */
  def reorder[T <: PigOperator : ClassTag, T2 <: PigOperator : ClassTag]:
  Unit = {
    val strategy = (parent: T, child: T2) =>
      Some(fixInputsAndOutputs(parent, child, child, parent))
    addBinaryPigOperatorStrategy(strategy)
  }

  /** Add a strategy that applies a function to two operators.
    *
    * @param f The function to apply.
    * @tparam T2 The second operators type.
    * @tparam T The first operators type.
    */
  def addBinaryPigOperatorStrategy[T2 <: PigOperator : ClassTag, T <: PigOperator : ClassTag](f: (T, T2)
    => Option[PigOperator]): Unit = {
    val strategy = buildBinaryPigOperatorStrategy(f)
    addStrategy(strategy)
  }

  /** Given a function `f: (T => Option[T])`, add a strategy that applies `f` if the input term is of type `T`.
    *
    * The type constraints here are stronger than on [[addInputTypedStrategy]] in that the type `T` is applied to
    * the input of `f` and its output.
    *
    * @param f
    * @tparam T
    */
  def addTypedStrategy[T <: PigOperator : ClassTag](f: (T => Option[T])): Unit = {
    val wrapper = buildTypedCaseWrapper(f)
    addStrategy(wrapper)
  }

  /** Given a function `f: (T => Option[Any])`, add a strategy that applies `f` if the input term is of type `T`.
    *
    * The type constraints here are weaker than in [[addTypedStrategy]] because they only apply to the type of `f`s
    * input.
    *
    * @param f
    * @tparam T
    */
  def addInputTypedStrategy[T <: PigOperator : ClassTag](f: (T => Option[PigOperator])): Unit = {
    val wrapper = buildTypedCaseWrapper(f)
    addStrategy(wrapper)
  }
}
