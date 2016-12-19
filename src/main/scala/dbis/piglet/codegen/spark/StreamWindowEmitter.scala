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
package dbis.piglet.codegen.spark

import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.op.PigOperator
import dbis.piglet.codegen.CodeGenException
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.op.Window

class StreamWindowEmitter extends CodeEmitter[Window] {
  override def template: String =
    """    val <out> = <in>.window(<wUnit>(<window>)<if (sUnit)>, <sUnit>(<slider>)<endif>)""".stripMargin

  override def code(ctx: CodeGenContext, op: Window): String = {
        if (op.window._2 == "") {
          if (op.slide._2 == "") render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "window" -> op.window._1, "slider" -> op.slide._1))
          else render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "window" -> op.window._1, "slider" -> op.slide._1, "sUnit" -> op.slide._2.toUpperCase))
        } else {
          if (op.slide._2 == "") render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "window" -> op.window._1, "wUnit" -> op.window._2.toUpperCase, "slider" -> op.slide._1))
          else render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "window" -> op.window._1, "wUnit" -> op.window._2.toUpperCase, "slider" -> op.slide._1, "sUnit" -> op.slide._2.toUpperCase))
        }
  }
}
