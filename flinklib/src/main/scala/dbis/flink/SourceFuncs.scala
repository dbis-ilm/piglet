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


import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source._
import org.apache.flink.streaming.api.functions.source.SourceFunction._
import org.zeromq._
import org.zeromq.ZMQ._

class ZmqSubscriber(addr: String) extends RichSourceFunction[List[java.io.Serializable]]{ 

  private var subscriber: Socket = _
  @volatile private var isRunning: Boolean = _
  private val schema = new UTF8StringSchema()

  @throws(classOf[Exception])
  override def open(parameters: Configuration) = {
    super.open(parameters)
    val context = ZMQ.context(1)
    subscriber = context.socket(ZMQ.SUB)
    subscriber.setRcvHWM(0)
    subscriber.connect(addr)
    subscriber.subscribe("".getBytes())
    isRunning = true;
  }

  @throws(classOf[Exception])
  override def run(ctx: SourceContext[List[java.io.Serializable]]) = {
    streamFromSocket(ctx, subscriber)
  }

  @throws(classOf[Exception])
  def streamFromSocket(ctx: SourceContext[List[java.io.Serializable]], socket: Socket) = {
    try {
      while (isRunning) {
        try {
          val msg: Array[Byte] = socket.recv(0)
          val element = msg match {
            case msg: Array[Byte] => schema.deserialize(msg)
            case _ => List(msg)
          }
//          val element: List[String] = schema.deserialize(msg)
          ctx.collect(element)
        } 
        catch {
          case e: ZMQException => throw e
        }
        
      }
    } finally {
      socket.close
    }
  }

  override def cancel() = {
    isRunning = false
    try {
      subscriber.close
    } catch {
      case e: java.io.IOException => throw new Exception(s"Could not close open socket")
    }
  }

}
