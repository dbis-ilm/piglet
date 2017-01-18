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
package dbis.piglet.codegen.flink

import dbis.piglet.codegen.scala_lang.ScalaEmitter
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.codegen.flink.emitter.OrderByEmitter
import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.op.OrderBy
import dbis.piglet.op.OrderBySpec
import dbis.piglet.schema.Schema
import dbis.piglet.op.OrderByDirection

class StreamOrderByEmitter extends OrderByEmitter {
  override def template: String = """""".stripMargin
  def templateHelper: String = """    .toList.sortBy(t => <key>)(<ordering>)""".stripMargin

  override def code(ctx: CodeGenContext, op: OrderBy): String = {
    val key = emitSortKey(CodeGenContext(ctx, Map("schema" -> op.schema, "tuplePrefix" -> "t")), op.orderSpec, op.outPipeName, op.inPipeName)
    val asc = ascendingSortOrder(op.orderSpec.head)
    render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "key" -> key, "asc" -> asc))
  }

  def windowApply(ctx: CodeGenContext, op: OrderBy): String = {
    var params = Map[String, Any]()
    params += "key" -> emitSortKey(CodeGenContext(ctx, Map("schema" -> op.schema, "tuplePrefix" -> "t")), op.orderSpec, op.outPipeName, op.inPipeName)
    params += "ordering" -> emitOrdering(op.schema, op.orderSpec)
    CodeEmitter.render(templateHelper, params)
  }

  /**
   * Creates the ordering definition for a given spec and schema.
   *
   * @param schema the schema of the order by operator
   * @param orderSpec the order specifications
   * @return the ordering definition
   */
  def emitOrdering(schema: Option[Schema], orderSpec: List[OrderBySpec]): String = {

    def emitOrderSpec(spec: OrderBySpec): String = {
      val reverse = if (spec.dir == OrderByDirection.DescendingOrder) ".reverse" else ""
      s"Ordering.${ScalaEmitter.scalaTypeOfField(spec.field, schema)}" + reverse
    }

    if (orderSpec.size == 1)
      emitOrderSpec(orderSpec.head)
    else
      s"Ordering.Tuple${orderSpec.size}(" + orderSpec.map { r => emitOrderSpec(r) }.mkString(",") + ")"
  }
  
    override def emitSortKey(ctx: CodeGenContext, orderSpec: List[OrderBySpec], out: String, in: String): String = {
    if (orderSpec.size == 1)
      ScalaEmitter.emitRef(ctx, orderSpec.head.field)
    else
      s"(${orderSpec.map(r => ScalaEmitter.emitRef(ctx, r.field)).mkString(",")})"
  }
}

object StreamOrderByEmitter {
	lazy val instance = new StreamOrderByEmitter
}