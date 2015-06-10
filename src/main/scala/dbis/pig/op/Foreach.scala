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

import dbis.pig.schema._


case class GeneratorExpr(expr: ArithmeticExpr, alias: Option[Field] = None)

/**
 * Foreach represents the FOREACH operator of Pig.
 *
 * @param initialOutPipeName the name of the output pipe (relation).
 * @param initialInPipeName the name of the input pipe
 * @param expr the generator expression
 */
case class Foreach(override val initialOutPipeName: String, initialInPipeName: String, expr: List[GeneratorExpr])
  extends PigOperator(initialOutPipeName, initialInPipeName) {

  override def constructSchema: Option[Schema] = {
    // val inputSchema = inputs.head.producer.schema
    // we create a bag of tuples containing fields for each expression in expr
    val fields = expr.map(e => {
      e.alias match {
        // if we have an explicit schema (i.e. a field) then we use it
        case Some(f) => {
          if (f.fType == Types.ByteArrayType) {
            // if the type was only bytearray, we should check the expression if we have a more
            // specific type
            val res = e.expr.resultType(inputSchema)
            Field(f.name, res._2)
          }
          else
            f
        }
        // otherwise we take the field name from the expression and
        // the input schema
        case None => val res = e.expr.resultType(inputSchema); Field(res._1, res._2)
      }
    }).toArray
    schema = Some(new Schema(new BagType("", new TupleType("", fields))))
    schema
  }

  override def checkSchemaConformance: Boolean = {
    // val inputSchema = inputs.head.producer.schema
    inputSchema match {
      case Some(s) => {
        // if we know the schema we check all named fields
        expr.map(_.expr.traverse(s, Expr.checkExpressionConformance)).foldLeft(true)((b1: Boolean, b2: Boolean) => b1 && b2)
      }
      case None => {
        // if we don't have a schema all expressions should contain only positional fields
        expr.map(_.expr.traverse(null, Expr.containsNoNamedFields)).foldLeft(true)((b1: Boolean, b2: Boolean) => b1 && b2)
      }
    }
  }

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""FOREACH%${expr}%""" + super.lineageString
  }
}

