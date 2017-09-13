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

import dbis.piglet.expr.Ref
import dbis.piglet.expr.PositionalField

object OrderByDirection extends Enumeration {
  type OrderByDirection = Value
  val AscendingOrder, DescendingOrder = Value
}


import dbis.piglet.op.OrderByDirection._

/**
 *
 * @param field The field on which is to be sorted
 * @param dir The direction (ascending or descending)
 */
case class OrderBySpec(field: Ref, dir: OrderByDirection) {
  override def toString = s"$field $dir"
}

case class OrderBy(
    private val out: Pipe, 
    private val in: Pipe, 
    orderSpec: List[OrderBySpec], 
    var windowMode: Boolean = false
  ) extends PigOperator(out, in) {

  override def lineageString: String = s"""ORDERBY%$orderSpec%""" + super.lineageString

  override def checkSchemaConformance: Boolean = {
    schema match {
      case Some(s) =>
        // if we know the schema we check all named fields
        // TODO
      case None =>
        // if we don't have a schema then the OrderBySpec list should contain only positional fields
        orderSpec.count(s => s.field match {
          case PositionalField(n) => true
          case _ => false
        }) == orderSpec.size
    }
    true
  }

  override def toString =
    s"""ORDER BY
       |  out = $outPipeName
       |  in = $inPipeName
       |  inSchema = $inputSchema
       |  outSchema = $schema
       |  by = ${orderSpec.mkString(",")}
     """.stripMargin

}
