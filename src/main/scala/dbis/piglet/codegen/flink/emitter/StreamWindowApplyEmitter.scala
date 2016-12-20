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

import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.op.WindowApply
import dbis.piglet.codegen.scala_lang.ScalaEmitter
import dbis.piglet.op.OrderBy
import dbis.piglet.op.PigOperator
import dbis.piglet.op.Pipe
import dbis.piglet.op.Distinct
import dbis.piglet.op.Empty
import dbis.piglet.op.Grouping
import dbis.piglet.op.Foreach
import dbis.piglet.op.Filter
import scala.collection.mutable
import dbis.piglet.codegen.scala_lang.FilterEmitter
import dbis.piglet.codegen.flink.emitter.StreamFilterEmitter
import dbis.piglet.codegen.flink.emitter.StreamForeachEmitter
import dbis.piglet.codegen.flink.emitter.StreamDistinctEmitter

class StreamWindowApplyEmitter extends CodeEmitter[WindowApply] {
  override def template: String = """    val <out> = <in>.apply(<func> _)"""

  override def code(ctx: CodeGenContext, op: WindowApply): String = {
    render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "func" -> op.fname))
  }

  override def helper(ctx: CodeGenContext, op: WindowApply): String = {
    val inSchema = ScalaEmitter.schemaClassName(op.inputSchema.get.className)
    val outSchema = ScalaEmitter.schemaClassName(op.schema.get.className)
    var fname, applyBody = ""
    var lastOp: PigOperator = new Empty(Pipe("empty"))
    val littleWalker = mutable.Queue(op.inputs.head.producer.outputs.flatMap(_.consumer).toSeq: _*)
    while (!littleWalker.isEmpty) {
      val operator = littleWalker.dequeue()
      operator match {
        case o @ Filter(_, _, pred, windowMode) if (windowMode) => {
          val e = new StreamFilterEmitter
          applyBody += e.windowApply(ctx, o) + "\n"
        }
        case o @ Distinct(_, _, windowMode) if (windowMode) => {
          val e = new StreamDistinctEmitter
          applyBody += e.windowApply(ctx, o) + "\n"
        }
        case o @ OrderBy(_, _, spec, windowMode) if (windowMode) => {
          val e = new StreamOrderByEmitter
          applyBody += e.windowApply(ctx, o) + "\n"
        }
        case o @ Grouping(_, _, groupExpr, windowMode) if (windowMode) => {
          val e = new StreamGroupingEmitter
          applyBody += e.windowApply(ctx, o) + "\n"
        }
        case o @ Foreach(_, _, gen, windowMode) if (windowMode) => {
          fname = "WindowFunc" + o.outPipeName
          val e = new StreamForeachEmitter
          applyBody += e.windowApply(ctx, o)
          return s"""  def ${fname}(wi: Window, ts: Iterable[${inSchema}], out: Collector[${outSchema}]) = {
                |    ts
                |${applyBody}
                |  }
                """.stripMargin
        }
        case _ =>
      }
      littleWalker ++= operator.outputs.flatMap(_.consumer)
      if (littleWalker.isEmpty) lastOp = operator
    }
    val before = lastOp.inputs.tail.head
    fname = "WindowFunc" + before.name
    applyBody += """.foreach { t => out.collect((t)) }"""
    s"""  def ${fname}(wi: Window, ts: Iterable[${inSchema}], out: Collector[${outSchema}]) = {
          |    ts
          |${applyBody}
          |  }
          """.stripMargin
  }
}
