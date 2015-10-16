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
package dbis.pig.plan.rewriting.dsl.builders

import dbis.pig.op.PigOperator
import dbis.pig.plan.rewriting.Rewriter

import org.kiama.rewriting.Rewriter.strategyf
import scala.reflect.{ClassTag, classTag}

/** Wraps strategy data, such as the rewriting function and pre-checks and adds strategies from them.
  *
  * @param func
  * @tparam FROM
  * @tparam TO
  */
class Builder[FROM <: PigOperator : ClassTag, TO: ClassTag](val func: (FROM => Option[TO])) {
  private var _check: Option[(FROM => Boolean)] = None

  def check_=(f: (FROM => Boolean)): Unit = _check = Some(f)

  def check = _check

  /** Add the data wrapped by this object as a strategy.
    *
    */
  def apply(): Unit = {
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

    val wrapped = Rewriter.buildTypedCaseWrapper(f)

    Rewriter.addStrategy(strategyf(t => wrapped(t)))
  }
}
