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

import dbis.pig.op.{Ref, Value, TriplePattern}
import dbis.pig.plan.rewriting.Column.Column

/** An enumeration of the three columns in RDF data - subject, predicate and object.
  *
  */
object Column extends Enumeration {
  type Column = Value
  val Subject, Predicate, Object = Value
}

object RDF {
  /** Returns true if no variable is bound by ``pattern``, false otherwise.
    *
    * @param pattern
    * @return
    */
  def allUnbound(pattern: TriplePattern): Boolean =
    (!pattern.subj.isInstanceOf[Value] && !pattern.pred.isInstanceOf[Value] && !pattern.obj.isInstanceOf[Value])

  /** Given a [[TriplePattern]] return the [[Column]] that's bound by a variable in it.
    *
    * If multiple variables are bound, [[None]] will be returned.
    *
    * @param pattern
    * @return
    */
  def getBoundColumn(pattern: TriplePattern): Option[Column] = {
    if (pattern.subj.isInstanceOf[Value]
      && !pattern.pred.isInstanceOf[Value]
      && !pattern.obj.isInstanceOf[Value] ) {
      Some(Column.Subject)
    } else if (!pattern.subj.isInstanceOf[Value]
      && pattern.pred.isInstanceOf[Value]
      && !pattern.obj.isInstanceOf[Value] ) {
      Some(Column.Predicate)
    } else if (!pattern.subj.isInstanceOf[Value]
      && !pattern.pred.isInstanceOf[Value]
      && pattern.obj.isInstanceOf[Value] ) {
      Some(Column.Object)
    } else {
      None
    }
  }

  /** Returns all [[Column]]s that are bound as a variable in ``pattern``.
    *
    * @param pattern
    * @return
    */
  def getAllBoundColumns(pattern: TriplePattern): List[Column] = {
    def isBound(r: Ref, c: Column): List[Column] = r match {
      case Value(_) => c :: Nil
      case _ => Nil
    }
    isBound(pattern.subj, Column.Subject) ++
      isBound(pattern.pred, Column.Predicate) ++
      isBound(pattern.obj, Column.Object)
  }
}
