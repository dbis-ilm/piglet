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

import dbis.piglet.schema.Schema
import dbis.piglet.expr.Ref

/**
 * A class representing the STREAM THROUGH operator for invoking a user-defined operator implemented
  * by an external Scala function.
  *
 * @param out the output pipe (relation).
 * @param in the input pipe
 * @param opName the name of the scala function
 * @param params an optional list of parameter values passed to the UDF
 * @param resSchema the optional result schema
 */
case class StreamOp(
    private val out: Pipe, 
    private val in: Pipe, 
    opName: String, 
    params: Option[List[Ref]] = None,
    var resSchema: Option[Schema] = None
  ) extends PigOperator(List(out), List(in), resSchema) {

  override def lineageString: String = s"""STREAM%${opName}%""" + super.lineageString

  override def checkSchemaConformance: Boolean = {
    // TODO
    true
  }

  override def constructSchema: Option[Schema] = {
    // if a result schema was defined we use it,
    // otherwise we assume that the UDF produces result with
    // the same schema
    if (schema.isEmpty)
      schema = inputSchema
    schema
  }

  override def printOperator(tab: Int): Unit = {
    println(indent(tab) + s"STREAM_THROUGH { out = ${outPipeName} , in = ${inPipeName} }")
    println(indent(tab + 2) + "inSchema = " + inputSchema)
    println(indent(tab + 2) + "outSchema = " + schema)
    println(indent(tab + 2) + "function = " + opName)
  }

}

