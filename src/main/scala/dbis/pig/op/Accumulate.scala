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

import dbis.pig.schema.{TupleType, BagType, Schema}
import dbis.pig.expr.ArithmeticExpr
import dbis.pig.expr.{Expr,Func}

/**
 * Accumulate represents the ACCUMULATE operator of Pig.
 *
 * @param out the output pipe (relation).
 * @param in the input pipe.
 * @param generator the generator (a list of aggregation expressions)
 */
case class Accumulate(
    out: Pipe, 
    in: Pipe, 
    generator: GeneratorList
  ) extends PigOperator(out, in) {

  override def lineageString: String = {
    s"""ACCUMULATE%${generator.exprs.mkString("%")}%""" + super.lineageString
  }

  override def constructSchema: Option[Schema] = {
    val fields = generator.constructFieldList(inputSchema)

    schema = Some(Schema(fields))
    schema
  }

  private def isValidParameter(params: List[ArithmeticExpr]): Boolean = {
    if (params.length != 1) return false
    val expr = params.head
    inputSchema match {
      case Some(s) => {
        // if we know the schema we check all named fields
        expr.traverseAnd(s, Expr.checkExpressionConformance)
      }
      case None => {
        // if we don't have a schema all expressions should contain only positional fields
        expr.traverseAnd(null, Expr.containsNoNamedFields)
      }
    }
  }

  override def checkSchemaConformance: Boolean = {
    // first we check whether all generator expressions are plain aggregate functions
    val funcCheck = generator.exprs.map{ e => e.expr match {
      case Func(f, params) => {
        val fname = f.toUpperCase
        (fname == "AVG" || fname == "SUM" || fname == "COUNT" || fname == "MIN" || fname == "MAX") && isValidParameter(params)
      }
      case _ => false
    }}
    ! funcCheck.contains(false)
    // TODO: second we check if the function arguments refer to valid fields
  }

  override def printOperator(tab: Int): Unit = {
    println(indent(tab) + s"ACCUMULATE { out = ${outPipeName} , in = ${inPipeName} }")
    println(indent(tab + 2) + "inSchema = " + inputSchema)
    println(indent(tab + 2) + "outSchema = " + schema)
    println(indent(tab + 2) + "exprs = " + generator.exprs.mkString(","))
  }
}