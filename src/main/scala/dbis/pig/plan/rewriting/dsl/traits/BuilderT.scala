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

/** A builder for rewriting operations.
  *
  * It wraps a function performing the rewriting in a conditional check and can automatically apply fixup operations
  * to the operations return value.
  *
  * Specific behaviour must be implemented by classes implementing this class.
  *
  * @tparam FROM
  * @tparam TO
  */
trait BuilderT[FROM, TO] {
  private var _func: Option[FROM => Option[TO]] = None

  def func_=(f: FROM => Option[TO]) = _func = Some(f)

  def func = _func

  private var _check: Option[FROM => Boolean] = None

  def check_=(f: FROM => Boolean): Unit = _check = Some(f)

  def check = _check

  def wrapInCheck(func: FROM => Option[TO]): FROM => Option[TO]

  def wrapInFixer(func: FROM => Option[TO]): FROM => Option[TO]

  def addAsStrategy(func: (FROM => Option[TO]))

  /** Add the data wrapped by this object as a strategy.
    *
    */
  def build(): Unit = {
    if(func.isEmpty) {
      throw new IllegalStateException("BuilderT.build called, but the function is not defined")
    }
    val wrapped = wrapInFixer(wrapInCheck(func.get))
    addAsStrategy(wrapped)
  }
}
