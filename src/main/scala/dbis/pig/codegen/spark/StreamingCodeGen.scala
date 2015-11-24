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
package dbis.pig.codegen.spark

import dbis.pig.backends.BackendManager
import dbis.pig.op._
import dbis.pig.codegen.CodeGenerator



class StreamingCodeGen(template: String) extends BatchCodeGen(template) {
  /**
    * Generates code for the WINDOW Operator
    *
    * @param out name of the output bag
    * @param in name of the input bag
    * @param window window size information (Num, Unit)
    * @param slide window slider information (Num, Unit)
    * @return the Scala code implementing the WINDOW operator
    */
  def emitWindow(out: String, in: String, window: Tuple2[Int, String], slide: Tuple2[Int, String]): String = {
    if(window._2==""){
      if(slide._2=="") callST("window", Map("out"-> out,"in"->in, "window"->window._1, "slider"->slide._1))
      else callST("window", Map("out"-> out,"in"->in, "window"->window._1, "slider"->slide._1, "sUnit"->slide._2))
    }
    else {
      if(slide._2=="") callST("window", Map("out"-> out,"in"->in, "window"->window._1, "wUnit"->window._2, "slider"->slide._1))
      else callST("window", Map("out"-> out,"in"->in, "window"->window._1, "wUnit"->window._2, "slider"->slide._1, "sUnit"->slide._2))
    }
  }

  /**
    * Generates code for the SOCKET_READ Operator
    *
    * @param node the PigOperator instance
    * @param addr the socket address to connect to
    * @param mode the connection mode, e.g. zmq or empty for standard sockets
    * @param streamFunc an optional stream function (we assume a corresponding Scala function is available)
    * @param streamParams an optional list of parameters to a stream function (e.g. separators)
    * @return the Scala code implementing the SOCKET_READ operator
    */
  def emitSocketRead(node: PigOperator, addr: SocketAddress, mode: String, streamFunc: Option[String], streamParams: List[String]): String ={
    var paramMap = super.emitExtractorFunc(node, streamFunc)

    val params = if (streamParams != null && streamParams.nonEmpty) ", " + streamParams.mkString(",") else ""
    val func = streamFunc.getOrElse(BackendManager.backend.defaultConnector)
    paramMap ++= Map("out" -> node.outPipeName, "addr_hostname" -> addr.hostname,
                      "addr_port" -> addr.port,
                      "func" -> func, "params" -> params)
    if (mode != "")
      paramMap += ("mode" -> mode)
    println("paramMap = " + paramMap)
    callST("socketRead", paramMap)
  }


  /*------------------------------------------------------------------------------------------------- */
  /*                           implementation of the GenCodeBase interface                            */
  /*------------------------------------------------------------------------------------------------- */

  /**
    * Generate code for the given Pig operator.
    *
    * @param node the operator (an instance of PigOperator)
    * @return a string representing the code
    */
  override def emitNode(node: PigOperator): String = {
    node match {
      case Window(out, in, window, slide) => emitWindow(node.outPipeName,node.inPipeName,window,slide)
      case SocketRead(out, address, mode, schema, func, params) => emitSocketRead(node, address, mode, func, params)
      case _ => super.emitNode(node)
    }
  }

}

class StreamingGenerator(templateFile: String) extends CodeGenerator {
  override val codeGen = new StreamingCodeGen(templateFile)
}
