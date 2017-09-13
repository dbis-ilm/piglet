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

import scala.collection.mutable.Map
import dbis.piglet.expr.Predicate
import dbis.piglet.expr.Ref
import dbis.piglet.expr.Expr
import dbis.piglet.expr.SpatialFilterPredicate
import dbis.piglet.op.IndexMethod.IndexMethod

import scala.collection.mutable

/**
 * SpatialFilter represents the SPATIALFILTER operator.
 *
 * @param out the output pipe (relation).
 * @param in the input pipe
 * @param pred the predicate used for filtering tuples from the input pipe
 */
case class SpatialFilter(
    private val out: Pipe, 
    private val in: Pipe, 
    pred: SpatialFilterPredicate,
    idx: Option[(IndexMethod, List[String])]
  ) extends PigOperator(out, in) {

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""SPATIALFILTER%$pred%$idx""" + super.lineageString
  }

  override def resolveReferences(mapping: mutable.Map[String, Ref]): Unit = pred.resolveReferences(mapping)

  override def checkSchemaConformance: Boolean = {
    schema match {
      case Some(s) =>
        // if we know the schema we check all named fields
        pred.traverseAnd(s, Expr.checkExpressionConformance)
      case None =>
        // if we don't have a schema all expressions should contain only positional fields
        pred.traverseAnd(null, Expr.containsNoNamedFields)
    }
  }


  override def toString =
    s"""SPATIALFILTER
       |  out = $outPipeName
       |  in = $inPipeName
       |  schema = $schema
       |  expr = $pred
       |  idx = $idx""".stripMargin


}
