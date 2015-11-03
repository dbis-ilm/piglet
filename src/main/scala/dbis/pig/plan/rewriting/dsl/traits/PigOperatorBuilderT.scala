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
package dbis.pig.plan.rewriting.dsl.traits

import dbis.pig.op.PigOperator
import dbis.pig.plan.rewriting.Rewriter

/** A builder for applying a rewriting operation to a [[dbis.pig.op.PigOperator]].
  *
  * @tparam FROM
  * @tparam TO
  */
abstract class PigOperatorBuilderT[FROM <: PigOperator, TO] extends BuilderT[FROM, TO] {
  override def wrapInCheck(func: FROM => Option[TO]) = {
    def f(term: FROM): Option[TO] = {
      if (check.isEmpty) {
        func(term)
      } else {
        if (check.get(term)) {
          func(term)
        } else {
          None
        }
      }
    }

    f _
  }

  override def wrapInFixer(func: FROM => Option[TO]): FROM => Option[TO] = {
    def f(term: FROM): Option[TO] = {
      func(term) map {t : TO => t match {
        case ret @ (a : PigOperator, b:PigOperator) =>
          Rewriter.fixReplacementwithMultipleOperators(term, a, b)
          t
        case op if op.isInstanceOf[PigOperator] =>
          val o = op.asInstanceOf[PigOperator]
          o.inputs = term.inputs
          o.asInstanceOf[TO]
        case _ =>
          t
      }}
    }

    f _
  }

  def addAsStrategy(func: (FROM => Option[TO]))

  /** Add the data wrapped by this object as a strategy.
    *
    */
  override def apply(): Unit = {
    val wrapped = wrapInFixer(wrapInCheck(func.get))
    addAsStrategy(wrapped)
  }
}
