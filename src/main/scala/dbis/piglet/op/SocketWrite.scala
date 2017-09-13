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

import dbis.piglet.backends.BackendManager

/**
 * Store represents the STORE operator of Pig.
 *
 * @param in the name of the input pipe
 * @param addr the socket address to write to
 * @param mode empty for standard socket or currently also possible "zmq"
 */
case class SocketWrite(
    private val in: Pipe,
    addr: SocketAddress,
    mode: String = "",
    func: Option[String] = None, //BackendManager.backend.defaultConnector
    params: List[String] = null) extends PigOperator(List(), List(in)) {

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""SOCKET_WRITE%$addr%$mode%""" + super.lineageString
  }

  override def toString =
    s"""SOCKET_WRITE
       |  in = $inPipeName
       |  addr = $addr
       |  mode = $mode
       |  func = $func
       |  params = ${params.mkString(",")}""".stripMargin
}
