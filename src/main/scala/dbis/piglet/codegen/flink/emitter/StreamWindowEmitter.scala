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
import dbis.piglet.op.PigOperator
import dbis.piglet.codegen.CodeGenException
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.op.Window
import dbis.piglet.op.Grouping

class StreamWindowEmitter extends CodeEmitter[Window] {
  override def template: String = """    val <out> = <in>.<type><if (unkeyed)>All<endif>(<\\>
                                    |<if(timeCount)>TumblingTimeWindows.of(Time.of(<window>, TimeUnit.<wUnit>))).trigger(CountTrigger.of(<slider>))
                                    |<elseif(countTime)>GlobalWindows.create()).trigger(ContinuousEventTimeTrigger.of(Time.of(<slider>, TimeUnit.<sUnit>))).evictor(CountEvictor.of(<window>))
                                    |<else><\\>
                                    |<if(wUnit)>Time.of(<window>, TimeUnit.<wUnit>)<\\>
                                    |<else><window><\\>
                                    |<endif><\\>
                                    |<if(slider)>, <if(sUnit)>Time.of(<slider>, TimeUnit.<sUnit>)<\\>
                                    |<else><slider><\\>
                                    |<endif><endif>)<\\>
                                    |<endif>
                                  """.stripMargin

  override def code(ctx: CodeGenContext, op: Window): String = {
    val isKeyed = op.inputs.head.producer.isInstanceOf[Grouping]
    val isTumbling = op.window == op.slide
    val windowIsTime = op.window._2 != ""
    val slideIsTime = op.slide._2 != ""
    var paramMap = Map("out" -> op.outPipeName, "in" -> op.inPipeName, "window" -> op.window._1)

    (isKeyed, isTumbling, windowIsTime, slideIsTime) match {
      // For Keyed Streams
      case (true, true, true, _) => render(paramMap ++ Map("type" -> "timeWindow", "wUnit" -> op.window._2.toUpperCase()))
      case (true, true, false, _) => render(paramMap ++ Map("type" -> "countWindow"))
      case (true, false, true, true) => render(paramMap ++ Map("type" -> "timeWindow", "wUnit" -> op.window._2.toUpperCase(), "slider" -> op.slide._1, "sUnit" -> op.slide._2.toUpperCase()))
      case (true, false, true, false) => render(paramMap ++ Map("type" -> "window", "timeCount" -> true, "wUnit" -> op.window._2.toUpperCase(), "slider" -> op.slide._1))
      case (true, false, false, true) => render(paramMap ++ Map("type" -> "window", "countTime" -> true, "slider" -> op.slide._1, "sUnit" -> op.slide._2.toUpperCase()))
      case (true, false, false, false) => render(paramMap ++ Map("type" -> "countWindow", "slider" -> op.slide._1))

      // For Unkeyed Streams
      case (false, true, true, _) => render(paramMap ++ Map("type" -> "timeWindow", "unkeyed" -> true, "wUnit" -> op.window._2.toUpperCase()))
      case (false, true, false, _) => render(paramMap ++ Map("type" -> "countWindow", "unkeyed" -> true))
      case (false, false, true, true) => render(paramMap ++ Map("type" -> "timeWindow", "unkeyed" -> true, "wUnit" -> op.window._2.toUpperCase(), "slider" -> op.slide._1, "sUnit" -> op.slide._2.toUpperCase()))
      case (false, false, true, false) => render(paramMap ++ Map("type" -> "window", "timeCount" -> true, "unkeyed" -> true, "wUnit" -> op.window._2.toUpperCase(), "slider" -> op.slide._1))
      case (false, false, false, true) => render(paramMap ++ Map("type" -> "window", "countTime" -> true, "unkeyed" -> true, "slider" -> op.slide._1, "sUnit" -> op.slide._2.toUpperCase()))
      case (false, false, false, false) => render(paramMap ++ Map("type" -> "countWindow", "unkeyed" -> true, "slider" -> op.slide._1))

      case _ => ???
    }
  }
}

object StreamWindowEmitter {
	lazy val instance = new StreamWindowEmitter
}