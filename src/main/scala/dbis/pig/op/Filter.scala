/*
 * Copyright (c) 2015 The Piglet team,
 *                    All Rights Reserved.
 *
 * This file is part of the Piglet package.
 *
 * PipeFabric is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License (GPL) as
 * published by the Free Software Foundation; either version 2 of
 * the License, or (at your option) any later version.
 *
 * This package is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; see the file LICENSE.
 * If not you can find the GPL at http://www.gnu.org/copyleft/gpl.html
 */
package dbis.pig.op

/**
 * Filter represents the FILTER operator of Pig.
 *
 * @param initialOutPipeName the name of the output pipe (relation).
 * @param initialInPipeName the name of the input pipe
 * @param pred the predicate used for filtering tuples from the input pipe
 */
case class Filter(override val initialOutPipeName: String, initialInPipeName: String, pred: Predicate)
  extends PigOperator(initialOutPipeName, initialInPipeName) {

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""FILTER%${pred}%""" + super.lineageString
  }

  override def checkSchemaConformance: Boolean = {
    schema match {
      case Some(s) => {
        // if we know the schema we check all named fields
        pred.traverse(s, Expr.checkExpressionConformance)
      }
      case None => {
        // if we don't have a schema all expressions should contain only positional fields
        pred.traverse(null, Expr.containsNoNamedFields)
      }
    }
  }


}
