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
package dbis.piglet.op
import dbis.piglet.schema._
import dbis.piglet.expr.Predicate
/**
 * @brief Matcher represents the complex event processing operator of Pig.
 */
/**
 * A pattern class represents the complex pattern for the matcher operator
 * where the engine should detect
 */
sealed abstract class Pattern
/**
 * An event class represents the simple event where the engine should detect
 * in order to detect the complex pattern
 */
sealed abstract class Event

/**
 * A class represents a simple pattern which consists only as a simple event name
 * @param name the name of simple event
 */
case class SimplePattern(name: String) extends Pattern
/**
 * A class represents the sequence pattern (i.e., complex one ) which has to consist other patterns. These
 * patterns can be simple or complex ones
 * @param patterns a list of patterns which the sequence pattern consists from
 */
case class SeqPattern(patterns: List[Pattern]) extends Pattern

/**
 * A class represents the conjunction pattern (i.e., complex one ) which has to consist other patterns. These
 * patterns can be simple or complex ones
 * @param patterns a list of patterns which the conjunction pattern consists from
 */

case class ConjPattern(patterns: List[Pattern]) extends Pattern

/**
 * A class represents the negation pattern. It receives only one pattern as its parameter to perform
 * the negation
 * @param patterns a list of patterns which the sequence pattern consists from
 */

case class NegPattern(patterns: Pattern) extends Pattern

/**
 * A class represents the disjunction pattern (i.e., complex one ) which has to consist other patterns. These
 * patterns can be simple or complex ones
 * @param patterns a list of patterns which the disjunction pattern consists from
 */

case class DisjPattern(patterns: List[Pattern]) extends Pattern

/**
 * A simple event represents the definition or the predicate of a particular simple pattern
 * @param simplePattern a simple pattern
 * @param predicate the predicate of the simple pattern
 */
case class SimpleEvent(simplePattern: Pattern, predicate: Predicate) extends Event

/**
 * A class represents all the definitions of the simple patterns in the operator. Through these definitions,
 * the simple patterns (i.e., events ) can be detected
 */
case class CompEvent(complex: List[SimpleEvent]) extends Event

case class Matcher(
    private val out: Pipe, in: Pipe,
    pattern: Pattern,
    events: CompEvent,
    mode: String = "skip_till_next_match",
    within: Tuple2[Int, String] = (0, "SECONDS")
  ) extends PigOperator(out) {

  /**
   * construct the schema of this operator. In general, this operator will not
   * change the schema of the previous operator in the chain. It will discard some tuples
   * without changing their structures.
   */
  override def constructSchema: Option[Schema] = {
    /*
     * Either the schema was defined or it is None.
     */
    schema
  }

  /**
   * Returns the lineage string for this operator.
   *
   * @return a string representation of the operator.
   */
  override def lineageString: String = {
    s"""Matcher""" + super.lineageString
  }
}
