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

import dbis.pig.plan.DataflowPlan
import dbis.pig.schema._


/**
 * A trait for the GENERATE part of a FOREACH operator.
 */
trait ForeachGenerator {}

/**
 * GeneratorExpr represents a single expression of a Generator.
 *
 * @param expr
 * @param alias
 */
case class GeneratorExpr(expr: ArithmeticExpr, alias: Option[Field] = None)

/**
 * GeneratorList implements the ForeachGenerator trait and is used to represent
 * the FOREACH ... GENERATE operator.
 *
 * @param exprs
 */
case class GeneratorList(exprs: List[GeneratorExpr]) extends ForeachGenerator

/**
 * GeneratorPlan implements the ForeachGenerator trait and is used to represent
 * a nested FOREACH.
 *
 * @param subPlan
 */
case class GeneratorPlan(subPlan: List[PigOperator]) extends ForeachGenerator

/**
 * Foreach represents the FOREACH operator of Pig.
 *
 * @param initialOutPipeName the name of the output pipe (relation).
 * @param initialInPipeName the name of the input pipe
 * @param generator the generator (a list of expressions or a subplan)
 */
case class Foreach(override val initialOutPipeName: String, initialInPipeName: String, generator: ForeachGenerator)
  extends PigOperator(initialOutPipeName, initialInPipeName) {

  var subPlan: Option[DataflowPlan] = None

  override def preparePlan: Unit = {
    /*
     * TODO: nested foreach require special handling
     * a) we have to add an input pipe
     * b) the final generate operator doesn't need an input pipe
     * c) we construct a subplan for the operator list
     */
    generator match {
      case GeneratorPlan(opList) => {
        // TODO:
        // println("----> generate subplan: " + opList)
        subPlan = Some(new DataflowPlan(opList))
      }
      case _ => {}
    }
  }

  override def constructSchema: Option[Schema] = {
    generator match {
      case GeneratorList(expr) => {
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
      }
      case GeneratorPlan(plan) => {
        // TODO: implement constructSchema for nested foreach
      }
    }
    schema
  }

  override def checkSchemaConformance: Boolean = {
    generator match {
      case GeneratorList(expr) => inputSchema match {
        case Some(s) => {
          // if we know the schema we check all named fields
          expr.map(_.expr.traverse(s, Expr.checkExpressionConformance)).foldLeft(true)((b1: Boolean, b2: Boolean) => b1 && b2)
        }
        case None => {
          // if we don't have a schema all expressions should contain only positional fields
          expr.map(_.expr.traverse(null, Expr.containsNoNamedFields)).foldLeft(true)((b1: Boolean, b2: Boolean) => b1 && b2)
        }
      }
      case GeneratorPlan(plan) => false // TODO: implement checkSchemaConformance for nested foreach
    }
  }

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    generator match {
      case GeneratorList(expr) => s"""FOREACH%${expr}%""" + super.lineageString
      case GeneratorPlan(plan) => s"""FOREACH""" + super.lineageString // TODO: implement lineageString for nested foreach
    }
  }
}

/**
 * GENERATE represents the final generate statement inside a nested FOREACH.
 *
 * @param exprs list of generator expressions
 */
case class Generate(exprs: List[GeneratorExpr]) extends PigOperator("") {
  // TODO: what do we need here?
}

/**
 * This operator is a pseudo operator used inside a nested FOREACH to construct a new bag from an expression.
 *
 * @param initialOutPipeName the name of the initial output pipe (relation) which is needed to construct the plan, but
 *                           can be changed later.
 * @param refExpr a reference referring to an expression constructing a relation (bag).
 */
case class ConstructBag(override val initialOutPipeName: String, refExpr: Ref) extends PigOperator (initialOutPipeName) {
  // TODO: what do we need here?
}