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

