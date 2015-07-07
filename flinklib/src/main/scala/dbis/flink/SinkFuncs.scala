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

package dbis.flink

import org.apache.flink.streaming.api.functions.sink._
import org.apache.flink.configuration.Configuration
import org.zeromq._
import org.zeromq.ZMQ._

class ZmqPublisher(addr: String) extends RichSinkFunction[List[String]]{ 
  private var publisher: Socket = _
  private val schema = new UTF8StringSchema()

  def initializeConnection = {
    try {
      printf("Initialize Publisher at Socket %s\n", addr)
      val context = ZMQ.context(1)
      publisher = context.socket(ZMQ.PUB)
      publisher.setLinger(5000);
      publisher.setSndHWM(0);
      publisher.bind(addr)
      Thread sleep 1000
    } catch {
      case e: java.io.IOException => throw new RuntimeException(s"Cannot initialize connection to socket $addr")
      case e: Throwable => throw e
    }
  }

  override def invoke(in: List[String]) = {
    val msg: Array[Byte] = schema.serialize(in)
    try {
      printf("Sending: %s\n", in.mkString(","))
      publisher.send(msg, 0)
    } catch {
      case e: java.io.IOException => throw new RuntimeException(s"Cannot send message ${in.toString} to socket $addr")
      case e: Throwable => throw e
    }
  }

  def closeConnection = {
    try {
      Thread sleep 1000
      publisher.close
    } catch {
      case e: java.io.IOException => throw new RuntimeException(s"Error while closing connection with socket $addr")
      case e: Throwable => throw e
    }
  }

  override def open(parameters: Configuration) = {
    initializeConnection
  }

  override def close = {
    closeConnection
  }
}
