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
 * A pipe connects some Pig operator and associates a name to this channel.
 *
 * @param name the name of the pipe
 * @param producer the operator producing the data
 * @param consumer the list of operators reading this data
 */
case class Pipe (var name: String, var producer: PigOperator = null, var consumer: List[PigOperator] = List()) {
  override def toString = s"Pipe($name)"

  override def hashCode = name.hashCode

  def inputSchema = if (producer != null) producer.schema else None
}
