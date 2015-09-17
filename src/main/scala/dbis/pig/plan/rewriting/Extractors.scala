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

import dbis.pig.op._

/** Provides extractor objects for [[dbis.pig.op.PigOperator]]s.
  *
  */
object Extractors {
  /** Extracts the name of the function called in a ForEach statement if the generator only calls that function
    *
    * Example:
    *
    * For the ForEach operator corresponding to the pig statement
    *
    * {{{
    *   B = FOREACH A GENERATE myFunc(f1, f2);
    * }}}
    *
    * this extractor returns "myFunc".
    */
  object ForEachCallingFunctionE {
    def unapply(f: Foreach): Option[String] = f match {
      case f @ Foreach(_, _, GeneratorList(List(GeneratorExpr(Func(funcname, _), _))), _) => Some(funcname)
      case _ => None
    }
  }

  /** Extracts the successor of ``op`` if there is only one.
    *
    */
  object OnlyFollowedByE {
    def unapply(op: PigOperator): Option[PigOperator] = {
      val suc = op.outputs.flatMap(_.consumer)
      if (suc.length == 1) {
        Some(suc.head)
      } else {
        None
      }
    }
  }
}
