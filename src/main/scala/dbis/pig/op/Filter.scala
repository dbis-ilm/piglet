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

package dbis.pig.op

import scala.collection.mutable.Map

/**
 * Filter represents the FILTER operator of Pig.
 *
 * @param out the output pipe (relation).
 * @param in the input pipe
 * @param pred the predicate used for filtering tuples from the input pipe
 * @param windowMode true if processed on a window on a data stream
 */
case class Filter(out: Pipe, in: Pipe, pred: Predicate, var windowMode: Boolean = false) extends PigOperator {
  _outputs = List(out)
  _inputs = List(in)

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""FILTER%${pred}%""" + super.lineageString
  }

  override def resolveReferences(mapping: Map[String, Ref]): Unit = pred.resolveReferences(mapping)

  override def checkSchemaConformance: Boolean = {
    schema match {
      case Some(s) => {
        // if we know the schema we check all named fields
        pred.traverseAnd(s, Expr.checkExpressionConformance)
      }
      case None => {
        // if we don't have a schema all expressions should contain only positional fields
        pred.traverseAnd(null, Expr.containsNoNamedFields)
      }
    }
  }


}
