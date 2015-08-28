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

import dbis.pig.op.{NamedField, Ref, Value, TriplePattern}
import dbis.pig.plan.rewriting.Column.Column
import scala.collection.mutable.Map

/** An enumeration of the three columns in RDF data - subject, predicate and object.
  *
  */
object Column extends Enumeration {
  type Column = Value
  val Subject, Predicate, Object = Value

  /** Maps [[dbis.pig.plan.rewriting.Column]]s to [[dbis.pig.op.NamedField]]s.
    *
    * @param c
    * @return
    */
  def columnToNamedField(c: Column) = c match {
    case Column.Subject => NamedField("subject")
    case Column.Predicate => NamedField("predicate")
    case Column.Object => NamedField("object")
  }
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

  /** Build a map of ([[Column]], [[NamedField]]) to the number of their occurences in ``patterns``.
    */
  private def buildStarJoinMap(patterns: List[TriplePattern]): Map[(Column.Value, NamedField), Int] = {
    // We count how often each (Column, NamedField) tuple appears in each pattern. If, at the end, a single tuple
    // appears as often as patterns is long, the same variable appears in the same position in each pattern,
    // therefore patterns form a star join.
    val variableInPosition  = Map[(Column.Value, NamedField), Int]().withDefaultValue(0)
    patterns foreach {pattern: TriplePattern =>
      if (pattern.subj.isInstanceOf[NamedField]) {
        val key = (Column.Subject, pattern.subj.asInstanceOf[NamedField])
        variableInPosition(key) = variableInPosition(key) + 1
      }

      if (pattern.pred.isInstanceOf[NamedField]) {
        val key = (Column.Subject, pattern.pred.asInstanceOf[NamedField])
        variableInPosition(key) = variableInPosition(key) + 1
      }

      if (pattern.obj.isInstanceOf[NamedField]) {
        val key = (Column.Subject, pattern.obj.asInstanceOf[NamedField])
        variableInPosition(key) = variableInPosition(key) + 1
      }
    }

    variableInPosition
  }

  /** Returns the [[dbis.pig.plan.rewriting.Column.Column]] and [[dbis.pig.op.NamedField]] of the star join column in ``patterns`` or None if it's not a star join.
    *
    * If two variables appear in the same position in all ``patterns``, None will be returned
    * @param patterns
    * @return
    */
  def starJoinColumn(patterns: List[TriplePattern]): Option[(Column, NamedField)] = {
    val variableInPosition = buildStarJoinMap(patterns)
    var column: Option[(Column, NamedField)] = None

    variableInPosition foreach { case ((col, namedfield), count) =>
     if (count == patterns.length) {
       if (column.isDefined) {
         // There's already a namedfield which appears pattern.length times in the same position
         return None
       }

       column = Some((col, namedfield))
     }
    }

    column
  }

    /** Returns true if ``patterns`` form a star join
      *
      * ``patterns`` form a star join if the same [[dbis.pig.op.NamedField]] is used in the same position in each
      * pattern.
      */
  def isStarJoin(patterns: List[TriplePattern]): Boolean = {
    buildStarJoinMap(patterns).foldLeft(false) { case (old, ((_, _), count)) =>
      old || count == patterns.length
    }
  }
}
