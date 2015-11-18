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

/** A trait supplying methods to set the function in a [[dbis.pig.plan.rewriting.dsl.traits.BuilderT]] and call its
  * apply method.
  */
trait EndWordT[FROM, TO] {
  val b: BuilderT[FROM, TO]

  /** Apply ``f`` (a total function) when rewriting.
    *
    * @param f
    */
  def applyRule(f: (FROM => Option[TO])): Unit = {
    b.func = f
    b.build()
  }

  /** Apply ``f`` (a partial function) when rewriting.
    *
    */
  def applyPattern(f: scala.PartialFunction[FROM, TO]): Unit = {
    val lifted = f.lift

    b.func = lifted
    b.build()
  }
}
