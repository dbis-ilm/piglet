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

import dbis.piglet.op.SocketRead
import dbis.piglet.codegen.CodeGenException
import dbis.piglet.backends.BackendManager
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.op.PigOperator
import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.codegen.scala_lang.ScalaEmitter

class StreamSocketReadEmitter extends CodeEmitter {
  override def template: String =
    """        val <out> = <func>[<class>]().receiveStream(ssc, "<addr_hostname>", <addr_port>, <extractor><if (params)><params><endif>)""".stripMargin

  override def code(ctx: CodeGenContext, node: PigOperator): String = {
    node match {
      case SocketRead(out, address, mode, schema, streamFunc, streamParams) => {
        var paramMap = ScalaEmitter.emitExtractorFunc(node, streamFunc)

        node.schema match {
          case Some(s) => paramMap += ("class" -> ScalaEmitter.schemaClassName(s.className))
          case None => paramMap += ("class" -> "Record")
        }

        val params = if (streamParams != null && streamParams.nonEmpty) ", " + streamParams.mkString(",") else ""
        val func = streamFunc.getOrElse(BackendManager.backend.defaultConnector)
        paramMap ++= Map("out" -> node.outPipeName, 
                         "addr_hostname" -> address.hostname,
                         "addr_port" -> address.port,
                         "func" -> func, 
                         "params" -> params)
        if (mode != "")
          paramMap += ("mode" -> mode)
        render(paramMap)
      }
      case _ => throw CodeGenException(s"unexpected operator: $node")
    }
  }
}
