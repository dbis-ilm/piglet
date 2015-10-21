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

import dbis.pig.plan.rewriting.dsl.words.WhenWord

trait CheckWordT[FROM, TO] {
  val b: BuilderT[FROM, TO]

  /** Add a check before the application of the function contained in the builder. If the check returns true, the
    * function will not be called.
    */
  def unless(check: (FROM => Boolean)): WhenWord[FROM, TO] = {
    val newCheck = {term: FROM =>
      (!check(term))
    }
    when(newCheck)
  }

  /** Add a check before the application of the function contained in the builder. If the check returns true, the
    * function will be called.
    */
  def when(check: (FROM => Boolean)): WhenWord[FROM, TO] = {
    new WhenWord(b, check)
  }

  /** Add a check before the application of the function contained in the builder that always calls the function.
    */
  def always: WhenWord[FROM, TO] = {
    new WhenWord(b, _ => true)
  }

  /** Add a check in the form of a pattern match before the application of the function contained in the builder. If
    * the pattern matches, the function will be called.
    *
    * Use it like
    *
    * {{{
    *   whenMatches { case _ : PigOperator => }
    * }}}
    */
  def whenMatches(check: scala.PartialFunction[FROM, _]) = {
    def f(term: FROM): Boolean = check.isDefinedAt(term)

    new WhenWord(b, f)
  }

  /** Add a check in the form of a pattern match before the application of the function contained in the builder. If
    * the pattern matches, the function will not be called.
    *
    * Use it like
    *
    * {{{
    *   unlessMatches { case _ : PigOperator => }
    * }}}
    */
  def unlessMatches(check: scala.PartialFunction[FROM, _]) = {
    def f(term: FROM): Boolean = !check.isDefinedAt(term)

    new WhenWord(b, f)
  }
}
