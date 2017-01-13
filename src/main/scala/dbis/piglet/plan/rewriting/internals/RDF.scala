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
package dbis.piglet.plan.rewriting.internals

import java.lang.IllegalArgumentException

import dbis.piglet.op._
import scala.collection.mutable.Map
import dbis.piglet.expr._

/** An enumeration of the three columns in RDF data - subject, predicate and object.
  *
  */
object Column extends Enumeration {
  type Column = Value
  val Subject, Predicate, Object = Value

  /** Maps [[Column]]s to [[dbis.piglet.op.NamedField]]s.
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
  import Column.Column
  /** Returns true if no variable is bound by ``pattern``, false otherwise.
    *
    * @param pattern
    * @return
    */
  def allUnbound(pattern: TriplePattern): Boolean =
    (!pattern.subj.isInstanceOf[Value] && !pattern.pred.isInstanceOf[Value] && !pattern.obj
      .isInstanceOf[Value])

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

  /** Extracts all variables as [[dbis.piglet.op.NamedField]] objects used in `patterns`.
    *
    * The order of the fields returned is '''arbitrary'''.
    *
    * @param patterns
    * @return
    */
  def getAllVariables(patterns: Seq[TriplePattern]): Set[NamedField] = {
    def isNamedField(r: Ref): Set[NamedField] = r match {
      case f @ NamedField(_, _) => Set(f)
      case _ => Set.empty
    }

    def namedFieldsOf(p: TriplePattern): Set[NamedField] =
      isNamedField(p.subj) ++
        isNamedField(p.pred) ++
        isNamedField(p.obj)
    import scala.language.postfixOps
    patterns flatMap namedFieldsOf toSet
  }

  /** Build a map of ([[Column]], [[NamedField]]) to the number of their occurences in ``patterns``.
    */
  private def buildStarJoinMap(patterns: Seq[TriplePattern]): Map[(Column.Value, NamedField), Int] = {
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
        val key = (Column.Predicate, pattern.pred.asInstanceOf[NamedField])
        variableInPosition(key) = variableInPosition(key) + 1
      }

      if (pattern.obj.isInstanceOf[NamedField]) {
        val key = (Column.Object, pattern.obj.asInstanceOf[NamedField])
        variableInPosition(key) = variableInPosition(key) + 1
      }
    }

    variableInPosition
  }

  /** Returns the [[Column.Column]] and [[dbis.piglet.op.NamedField]] of the star join column in ``patterns`` or None if it's not a star join.
    *
    * If two variables appear in the same position in all ``patterns``, None will be returned
    * @param patterns
    * @return
    */
  def starJoinColumn(patterns: Seq[TriplePattern]): Option[(Column, NamedField)] = {
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
    * ``patterns`` form a star join if the same [[dbis.piglet.op.NamedField]] is used in the same position in each
    * pattern.
    */
  def isStarJoin(patterns: Seq[TriplePattern]): Boolean =
    (patterns.length > 1) && buildStarJoinMap(patterns).foldLeft(false) { case (old, (
      (_, _), count)) =>
      old || count == patterns.length
    }

  /** Builds a map of [[dbis.piglet.op.NamedField]] objects to the columns they appear in in `patterns`.
    *
    */
  private def buildPathJoinMap(patterns: Seq[TriplePattern]): Map[NamedField, Set[Column.Value]] = {
    val pathJoinMap = Map[NamedField, Set[Column.Value]]().withDefaultValue(Set.empty)
    patterns foreach { pattern =>
      if (pattern.subj.isInstanceOf[NamedField]) {
        val key = pattern.subj.asInstanceOf[NamedField]
        pathJoinMap(key) = pathJoinMap(key) + Column.Subject
      }

      if (pattern.pred.isInstanceOf[NamedField]) {
        val key = pattern.pred.asInstanceOf[NamedField]
        pathJoinMap(key) = pathJoinMap(key) + Column.Predicate
      }

      if (pattern.obj.isInstanceOf[NamedField]) {
        val key = pattern.obj.asInstanceOf[NamedField]
        pathJoinMap(key) = pathJoinMap(key) + Column.Object
      }
    }
    pathJoinMap
  }

  /** Returns the [[dbis.piglet.op.NamedField]] that's used in the path join in ``patterns``.
    */
  def pathJoinNamedField(patterns: Seq[TriplePattern]): Option[(NamedField)] = {
    buildPathJoinMap(patterns).find(_._2.size == patterns.length).map(_._1)
  }

  /** True if `patterns` form a path join.
    *
    */
  def isPathJoin(patterns: Seq[TriplePattern]): Boolean =
    (patterns.length > 1) && buildPathJoinMap(patterns).foldLeft(false) { case (old, (_, set)) =>
      old || set.size == patterns.length
    }

  /** Converts multiple TriplePatterns to a String representation as a BGP.
    *
    * @param patterns
    * @return
    */
  def triplePatternsToString(patterns: Seq[TriplePattern]): String = {
    def columnToString(column: Ref): String = column match {
      case NamedField(n, _) => s"?$n"
      case PositionalField(p) => s"$$$p"
      case Value(v) => s"""$v"""
      case _ => throw new IllegalArgumentException("Can't handle " + column)
    }
    "{ " + patterns.map { p: TriplePattern =>
      columnToString(p.subj) + " " + columnToString(p.pred) + " " + columnToString(p.obj)
    }.mkString(" . ") + " }"
  }

  /** Builds a new [[dbis.piglet.op.Eq]] constraint that filters by the value of `column` in `p`.
    *
    * @param column
    * @param p
    * @return
    */
  def columnToConstraint(column: Column, p: TriplePattern): Eq = {
    val filter_by = Column.columnToNamedField(column)
    val filter_value = column match {
      case Column.Subject => p.subj
      case Column.Predicate => p.pred
      case Column.Object => p.obj
    }
    return Eq(RefExpr(filter_by), RefExpr(filter_value))
  }

  /** Builds a [[dbis.piglet.op.Predicate]] that checks for all the bound columns in `p`.
   *
   * @param p
   * @return None, if no columns are bound.
   */
  def patternToConstraint(p: TriplePattern): Option[Predicate] = {
    val bound_columns = getAllBoundColumns(p)
    val constraints = bound_columns map {c => columnToConstraint(c, p)}
    constraints.length match {
      case 0 => None
      case 1 => Some(constraints.head)
      case _ => Some(constraints reduceLeft And)
    }
  }

  def namedFieldColumnPairFromPattern(p: TriplePattern): Seq[(NamedField, Column.Column)] = {
    def makeTuple(r: Ref, c: Column): Option[(NamedField, Column.Column)] = {
      if (r.isInstanceOf[NamedField]) {
        return Some((r.asInstanceOf[NamedField], c))
      }
      None
    }

    List(makeTuple(p.subj, Column.Subject),
      makeTuple(p.pred, Column.Predicate),
      makeTuple(p.obj, Column.Object)).flatten
  }
}
