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

import dbis.pig.plan.{InvalidPlanException, DataflowPlan}
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
    // nested foreach require special handling: we construct a subplan for the operator list
    generator match {
      case GeneratorPlan(opList) => {
        subPlan = Some(new DataflowPlan(opList))
      }
      case _ => {}
    }
  }

  def constructFieldList(exprs: List[GeneratorExpr]): Array[Field] =
    exprs.map(e => {
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

  override def constructSchema: Option[Schema] = {
    generator match {
      case GeneratorList(expr) => {
        val fields = constructFieldList(expr)

        schema = Some(new Schema(new BagType("", new TupleType("", fields))))
      }
      case GeneratorPlan(plan) => {
        val genOp = plan.last
        if (genOp.isInstanceOf[Generate]) {
          val exprs = genOp.asInstanceOf[Generate].exprs
          val fields = constructFieldList(exprs)
          schema = Some(new Schema(new BagType("", new TupleType("", fields))))
        }
        else
          throw new InvalidPlanException("last statement in nested foreach must be a generate")
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
      case GeneratorPlan(plan) => true // TODO: implement checkSchemaConformance for nested foreach
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