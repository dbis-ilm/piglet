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
package dbis.pig.plan.rewriting.rulesets

import dbis.pig.op._
import dbis.pig.plan.rewriting.Rewriter._
import org.kiama.rewriting.Rewriter._

object SparkRuleset extends Ruleset {
  val isGrouping: PartialFunction[Any, Option[Boolean]] = {
    case _ : Foreach | _ : Limit | _ : StreamOp => None
    case op => Some(op.isInstanceOf[Grouping])
  }

  /** Remove [[dbis.pig.op.OrderBy]] operators that are at some point followed by a [[dbis.pig.op.Grouping]].
    *
    * groupBy operations in spark are explicitly documented to not preserve order
    */
  def removeOrderByFollowedByGroupBy = rulefs[OrderBy] {
    case orderby: OrderBy =>
      val canRemove = everything("findGroupBy", Some(false): Option[Boolean]) { (old: Option[Boolean], new_ :
    Option[Boolean]) =>
        new_ match {
          case None => None
          case Some(v) => old.map(_ || v)
        }
      } (isGrouping) (orderby)
      if (canRemove.isDefined && canRemove.get) {
        manybu(buildRemovalStrategy(orderby))
      } else {
        fail
      }
  }

  override def registerRules(): Unit = {
    toMerge[OrderBy, Limit]() applyPattern {
      case (o @ OrderBy(_, in, spec, _), l @ Limit(out, _, num)) => Top(out, in, spec, num)
    }

    addStrategy(removeOrderByFollowedByGroupBy)
  }
}
