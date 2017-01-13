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

import dbis.piglet.codegen.scala_lang.GroupingEmitter
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.op.Grouping
import dbis.piglet.codegen.scala_lang.ScalaEmitter
import dbis.piglet.codegen.CodeEmitter

class StreamGroupingEmitter extends GroupingEmitter {
  override def template: String = """<if (expr)>
                                    |    val <out> = <in>.map(t => <class>(<expr>, List(t))).keyBy(t => t._0)
                                    |<else>
                                    |    val <out> = <in>.map(t => <class>("all", List(t))).keyBy(t => t._0)
                                    |<endif>""".stripMargin

  def templateHelper: String = """    .groupBy(t => <expr>).map(t => <class>(t._1,t._2))"""

  def windowApply(ctx: CodeGenContext, op: Grouping): String = {
    var params = Map[String, Any]()
    params += "expr" -> ScalaEmitter.emitGroupExpr(CodeGenContext(ctx, Map("schema" -> op.inputSchema, "tuplePrefix" -> "t")), op.groupExpr)
    params += "class" -> ScalaEmitter.schemaClassName(op.schema.get.className)
    CodeEmitter.render(templateHelper, params)
  }

  override def code(ctx: CodeGenContext, op: Grouping): String = {
    if (op.windowMode) return ""
    require(op.schema.isDefined)
    val className = ScalaEmitter.schemaClassName(op.schema.get.className)
    if (op.groupExpr.keyList.isEmpty)
      render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "class" -> className))
    else
      render(Map("out" -> op.outPipeName, "in" -> op.inPipeName,
        "expr" -> ScalaEmitter.emitGroupExpr(CodeGenContext(ctx, Map("schema" -> op.inputSchema, "tuplePrefix" -> "t")), op.groupExpr), "class" -> className))

  }
}