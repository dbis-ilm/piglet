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

import dbis.pig.schema.Schema

case class SocketAddress(protocol: String, hostname: String, port: String)
/**
 * Load represents the LOAD operator of Pig.
 *
 * @param out the name of the initial output pipe (relation).
 * @param addr the address of the socket to read from
 * @param mode empty for standard socket or currently also possible "zmq"
 * @param streamSchema schema definition 
 * @param streamFunc name of the stream function used for data preprocessing
 * @param streamParams parameters for streamFunc
 */
case class SocketRead(out: Pipe, addr: SocketAddress, mode: String, 
                      var streamSchema: Option[Schema] = None, 
                      streamFunc: String = "", streamParams: List[String] = null) extends PigOperator{
  _outputs = List(out)
  _inputs = List()
  schema = streamSchema

  override def constructSchema: Option[Schema] = {
    /*
     * Either the schema was defined or it is None.
     */
    schema
  }

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""SOCKET_READ%${addr}%${mode}""" + super.lineageString
  }
}
