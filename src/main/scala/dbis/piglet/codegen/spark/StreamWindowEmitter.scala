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

class StreamWindowEmitter extends CodeEmitter {
  override def template: String =
    """    val <out> = <in>.window(<wUnit>(<window>)<if (sUnit)>, <sUnit>(<slider>)<endif>)""".stripMargin

  override def code(ctx: CodeGenContext, node: PigOperator): String = {
    node match {
      case Window(out, in, window, slide) => {
        if (window._2 == "") {
          if (slide._2 == "") render(Map("out" -> out.name, "in" -> in.name, "window" -> window._1, "slider" -> slide._1))
          else render(Map("out" -> out.name, "in" -> in.name, "window" -> window._1, "slider" -> slide._1, "sUnit" -> slide._2.toUpperCase))
        } else {
          if (slide._2 == "") render(Map("out" -> out.name, "in" -> in.name, "window" -> window._1, "wUnit" -> window._2.toUpperCase, "slider" -> slide._1))
          else render(Map("out" -> out.name, "in" -> in.name, "window" -> window._1, "wUnit" -> window._2.toUpperCase, "slider" -> slide._1, "sUnit" -> slide._2.toUpperCase))
        }
      }
      case _ => throw CodeGenException(s"unexpected operator: $node")
    }
  }
}
