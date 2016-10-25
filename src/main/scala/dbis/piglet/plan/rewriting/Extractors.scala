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
package dbis.piglet.plan.rewriting

import dbis.piglet.op._
import dbis.piglet.expr.Func

import scala.reflect.ClassTag

/** Provides extractor objects for [[dbis.piglet.op.PigOperator]]s.
  *
  * The first object returned by any unapply method in this object is always the input itself.
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
    * the pattern
    *
    * {{{
    *   case ForEachCallingFunctionE(op, "myFunc") =>
    * }}}
    *
    * will match bind ``op`` to the [[dbis.piglet.op.Foreach]] operator if the function called in its ``GENERATE``
    * statement is ``myFunc``.
    */
  object ForEachCallingFunctionE {
    def unapply(f: Foreach): Option[(PigOperator, String)] = f match {
      case f @ Foreach(_, _, GeneratorList(List(GeneratorExpr(Func(funcname, _), _))), _) => Some((f, funcname))
      case _ => None
    }
  }

  /** Extracts the successor of ``op`` if there is only one.
    *
    * The pattern
    *
    * {{{
    *   case SuccE(op, succ) =>
    * }}}
    *
    * will bind ``op`` to a [[dbis.piglet.op.PigOperator]] object and ``succ`` to its successor.
    *
    */
  object SuccE {
    def unapply(op: PigOperator): Option[(PigOperator, PigOperator)] = {
      val suc = op.outputs.flatMap(_.consumer)
      if (suc.length == 1) {
        Some((op, suc.head))
      } else {
        None
      }
    }
  }

  /** Extracts all successors of ``op``.
    *
    * The pattern
    *
    * {{{
    *  case AllSuccE(op, succs) =>
    * }}}
    *
    * will bind ``op`` to a [[dbis.piglet.op.PigOperator]] object and ``succs`` to its successors.
    *
    * Of course, this can be combined with other patterns like
    *
    * {{{
    *  case AllSuccE(op, first :: second) =>
    * }}}
    *
    * to only match and bind ``op`` if it has only two successors, namely ``first`` and ``second``.
    *
    */
  object AllSuccE {
    def unapply(op: PigOperator): Option[(PigOperator, Seq[PigOperator])] = {
      val succ = op.outputs.flatMap(_.consumer)
      Some((op, succ))
    }
  }

  /** Extracts the predecessor of ``op`` if there is only one.
    *
    * The pattern
    *
    * {{{
    *   case PredE(op, pred) =>
    * }}}
    *
    * will bind ``op`` to a [[dbis.piglet.op.PigOperator]] object and ``pred`` to its successor.
    *
    */
  object PredE {
    def unapply(op: PigOperator): Option[(PigOperator, PigOperator)] = {
      val preds = op.inputs.map(_.producer)
      if (preds.length == 1) {
        Some((op, preds.head))
      } else {
        None
      }
    }
  }

  /** Extracts all predecessors of ``op``.
    *
    * The pattern
    *
    * {{{
    *  case AllPredE(op, preds) =>
    * }}}
    *
    * will bind ``op`` to a [[dbis.piglet.op.PigOperator]] object and ``preds`` to its predecessors.
    *
    * Of course, this can be combined with other patterns like
    *
    * {{{
    *  case AllPredE(op, first :: second) =>
    * }}}
    *
    * to only match and bind ``op`` if it has only two successors, namely ``first`` and ``second``.
    *
    */
  object AllPredE {
    def unapply(op: PigOperator): Option[(PigOperator, Seq[PigOperator])] = {
      val preds = op.inputs.map(_.producer)
      Some((op, preds))
    }
  }
}
