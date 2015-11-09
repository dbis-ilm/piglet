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
import dbis.pig.plan.rewriting.dsl.traits.BuilderT

import scala.reflect.ClassTag

/** A builder for applying a rewriting operation to two [[dbis.pig.op.PigOperator]]s. The rewriting operation must
  * merge them by returning a single new operator.
  *
  */
class MergeBuilder[FROM1 <: PigOperator : ClassTag, FROM2 <: PigOperator : ClassTag]
  extends BuilderT[(FROM1, FROM2), PigOperator] {
  override def addAsStrategy(func: ((FROM1, FROM2)) => Option[PigOperator]): Unit = {
    Rewriter.merge[FROM1, FROM2]{ (term1: FROM1, term2: FROM2) => func((term1, term2))}
  }

  override def wrapInCheck(func: ((FROM1, FROM2)) => Option[PigOperator]):
   ((FROM1, FROM2)) => Option[PigOperator] = {
    def wrapped(t: Tuple2[FROM1, FROM2]): Option[PigOperator] = {
      if (check.isEmpty || check.get(t)) {
        func(t)
      }
      else {
        None
      }
    }

    wrapped
  }

  override def wrapInFixer(func: ((FROM1, FROM2)) => Option[PigOperator]): ((FROM1, FROM2)) =>
    Option[dbis.pig.op.PigOperator] =
    func
}
