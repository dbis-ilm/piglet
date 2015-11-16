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

import dbis.pig.expr.Ref
import dbis.pig.expr.PositionalField

object OrderByDirection extends Enumeration {
  type OrderByDirection = Value
  val AscendingOrder, DescendingOrder = Value
}


import dbis.pig.op.OrderByDirection._

/**
 *
 * @param field
 * @param dir
 */
case class OrderBySpec(field: Ref, dir: OrderByDirection)

/**
 *
 * @param initialOutPipeName the name of the initial output pipe (relation) which is needed to construct the plan, but
 *                           can be changed later.
 * @param initialInPipeName
 * @param orderSpec
 */
case class OrderBy(out: Pipe, in: Pipe, orderSpec: List[OrderBySpec], var windowMode: Boolean = false) extends PigOperator {
  _outputs = List(out)
  _inputs = List(in)

  override def lineageString: String = s"""ORDERBY%${orderSpec}%""" + super.lineageString

  override def checkSchemaConformance: Boolean = {
    schema match {
      case Some(s) => {
        // if we know the schema we check all named fields
        // TODO
      }
      case None => {
        // if we don't have a schema then the OrderBySpec list should contain only positional fields
        orderSpec.filter(s => s.field match {
          case PositionalField(n) => true
          case _ => false
        }).size == orderSpec.size
      }
    }
    true
  }

  override def printOperator(tab: Int): Unit = {
    println(indent(tab) + s"ORDER BY { out = ${outPipeName} , in = ${inPipeName} }")
    println(indent(tab + 2) + "inSchema = " + inputSchema)
    println(indent(tab + 2) + "outSchema = " + schema)
    println(indent(tab + 2) + "by = " + orderSpec.mkString(","))
  }
}
