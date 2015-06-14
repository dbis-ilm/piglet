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
package dbis.pig.plan.rewriting

import dbis.pig.op.{And, Filter, PigOperator}
import dbis.pig.plan.Pipe
import org.kiama.rewriting.Rewriter._
import org.kiama.rewriting.Strategy

object Rewriter {
  private var strategy = fail

  /** Add a [[org.kiama.rewriting.Strategy]] to this Rewriter.
   *
   * It will be added by [[org.kiama.rewriting.Rewriter.ior]]ing it with the already existing ones.
   * @param s The new strategy.
   */
  def addStrategy(s: Strategy) = {
    strategy = ior(strategy, s)
  }

  /** Rewrites a given sink node with several [[org.kiama.rewriting.Strategy]]s that were added via
   * [[dbis.pig.plan.rewriting.Rewriter.addStrategy]].
   *
   * @param sink The sink node to rewrite.
   * @return The rewritten sink node.
   */
  def processSink(sink: PigOperator): PigOperator = {
    val rewriter = bottomup( attempt (strategy))
    rewrite(rewriter)(sink)
  }

  private def mergeFilters(pigOperator: Any): Option[Filter] = pigOperator match {
    case f1 @ Filter(out, _, predicate) =>
      f1.inputs match {
        case List((Pipe(_, f2 @ Filter(_, in, predicate2)))) =>
          val newFilter = Filter(out, in, And(predicate, predicate2))
          // TODO extract merging PigOperators into a new method, possibly on PigOperator itself
          // Use the second filters inputs. We don't need to rewrite the inputs output because that always seems to be
          // pointing to the input itself?
          newFilter.inputs = f2.inputs
          // TODO can there be other objects that have f1 in their input list?
          Some(newFilter)
        case _ => None
      }
    case _ => None
  }

  addStrategy(strategyf(t => mergeFilters(t)))
}
