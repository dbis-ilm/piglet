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

/**
 *
 * @param out the output pipe (relation).
 * @param in the input pipe.
 * @param expr an expression producing the sample size.
 */
case class Sample(out: Pipe, in: Pipe, expr: ArithmeticExpr) extends PigOperator {

  _outputs = List(out)
  _inputs = List(in)

  override def lineageString: String = s"""SAMPLE%${expr}%""" + super.lineageString

  override def checkSchemaConformance: Boolean = {
    schema match {
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
}
