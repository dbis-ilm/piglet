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
package dbis.pig.plan.rewriting.dsl

import dbis.pig.op.PigOperator
import dbis.pig.plan.rewriting.dsl.builders.{MergeBuilder, ReplacementBuilder, Builder}
import dbis.pig.plan.rewriting.dsl.traits.{CheckWordT, EndWordT}
import dbis.pig.plan.rewriting.dsl.words._

import scala.reflect._

/** The entry point to the rewriting DSL.
  *
  */
trait RewriterDSL {
  /** Start describing a replacement process for objects of type ``cls``.
    *
    * @param cls
    * @tparam FROM
    * @return
    */
  def toReplace[FROM <: PigOperator : ClassTag](cls: Class[FROM]): ReplaceWord[FROM] = {
    val b = new ReplacementBuilder[FROM, PigOperator]
    new ReplaceWord[FROM](b)
  }

  /** Start describing a merging operation for objects of type ``cls1`` and ``cls2``.
    *
    * Example code for merging two [[dbis.pig.op.Filter]] operators:
    *
    * {{{
    * Rewriter toMerge(classOf[Filter], classOf[Filter])  applyRule {
    *    tup: (Filter, Filter) => Some(Filter(tup._2.outputs.head, tup._1.inputs.head, And(tup._1.pred, tup._2.pred)))
    *  }
    * }}}
    * @param cls1
    * @param cls2
    * @tparam FROM1
    * @tparam FROM2
    * @return
    */
  def toMerge[FROM1 <: PigOperator : ClassTag, FROM2 <: PigOperator : ClassTag]
    (cls1: Class[FROM1], cls2: Class[FROM2]): MergeWord[FROM1, FROM2] = {
    val b = new MergeBuilder[FROM1, FROM2]
    new MergeWord[FROM1, FROM2](b)
  }

  /** Unconditionally apply ``f`` when rewriting.
    *
    * @param f
    * @tparam FROM
    * @tparam TO
    */
  def applyRule[FROM <: PigOperator : ClassTag, TO: ClassTag](f: (FROM => Option[TO])): Unit = {
    val b = new Builder[FROM, TO]
    new ImmediateEndWord(b).applyRule(f)
  }

  /** Unconditionally apply ``f`` when rewriting.
    *
    * @param f
    * @tparam TO
    */
  def applyPattern[TO: ClassTag](f: scala.PartialFunction[PigOperator, TO]): Unit = {
    val b = new Builder[PigOperator, TO]
    new ImmediateEndWord(b).applyPattern(f)
  }


  /** Start describing a rewriting process by supplying a check that needs to fail to make the rewriting happen.
    *
    * @param check
    * @tparam FROM
    * @tparam TO
    * @return
    */
  def unless[FROM <: PigOperator : ClassTag, TO : ClassTag](check: (FROM => Boolean)): WhenWord[FROM, TO] = {
    val b = new Builder[FROM, TO]
    def newcheck(term: FROM): Boolean = !check(term)
    new WhenWord[FROM, TO](b, newcheck)
  }

  /** Start describing a rewriting process by supplying a check that needs to succeed to make the rewriting happen.
    *
    * @param check
    * @tparam FROM
    * @tparam TO
    * @return
    */
  def when[FROM <: PigOperator : ClassTag, TO : ClassTag](check: (FROM => Boolean)): WhenWord[FROM, TO] = {
    val b = new Builder[FROM, TO]
    new WhenWord[FROM, TO](b, check)
  }

  /** Start describing a rewriting process by supplying a check in the form of a pattern match that needs to succeed to
    * make the rewriting happen.
    *
    * Use it like
    *
    * {{{
    *   whenMatches { case _ : PigOperator => }
    * }}}
    */
  def whenMatches[FROM <: PigOperator : ClassTag, TO : ClassTag](check: scala.PartialFunction[FROM, _]) = {
    val b = new Builder[FROM, TO]
    def f(term: FROM): Boolean = check.isDefinedAt(term)

    new WhenWord(b, f)
  }

  /** Start describing a rewriting process by supplying a check in the form of a pattern match that needs to fail to
    * make the rewriting happen.
    *
    * Use it like
    *
    * {{{
    *   whenMatches { case _ : PigOperator => }
    * }}}
    */
  def unlessMatches[FROM <: PigOperator : ClassTag, TO : ClassTag](check: scala.PartialFunction[FROM, _]) = {
    val b = new Builder[FROM, TO]
    def f(term: FROM): Boolean = !check.isDefinedAt(term)

    new WhenWord(b, f)
  }
}
