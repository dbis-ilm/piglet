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

import dbis.piglet.codegen.scala_lang.GroupingEmitter

class StreamGroupingEmitter extends GroupingEmitter {
  override def template: String = """<if (expr)>
                                    |    val <out> = <in>.transform(rdd => rdd.groupBy(t => {<expr>}).map{case (k,v) => <class>(<keyExtr>,v)})
                                    |<else>
                                    |    val <out> = <in>.transform(rdd => rdd.coalesce(1).glom.map(t => <class>("all", t)))
                                    |<endif>""".stripMargin
}

object StreamGroupingEmitter {
	lazy val instance = new StreamGroupingEmitter
}