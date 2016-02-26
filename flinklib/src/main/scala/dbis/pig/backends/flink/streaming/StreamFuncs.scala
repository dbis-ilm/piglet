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

package dbis.pig.backends.flink.streaming

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import scala.reflect.ClassTag
import dbis.pig.backends._


class PigStream [T <: SchemaClass :ClassTag: TypeInformation] extends java.io.Serializable {
 
  def loadStream (env: StreamExecutionEnvironment,  path: String,  extract: (Array[String]) => T, delim: String = " "): DataStream[T] = {
    env.readTextFile(path).setParallelism(1).map(line => extract(line.split(delim, -1)))
  }

  def writeStream(path: String, result: DataStream[T], delim: String = ",") = result.map(_.mkString(delim)).writeAsText(path).setParallelism(1)
  
  
   /*
  def connect(env: StreamExecutionEnvironment, host: String, port: Int, delim: Char = '\t'): DataStream[List[String]] = {
    env.socketTextStream(host,port).map(line => line.split(delim).toList)
  }

  def bind(host: String, port: Int, result: DataStream[List[String]]) = {
    result.writeToSocket(host, port, new UTF8StringSchema())
  }

  def zmqSubscribe(env: StreamExecutionEnvironment, addr: String, delim: Char = '\t'): DataStream[List[String]] = {
    env.addSource(new ZmqSubscriber(addr)).map(line => line.split(delim).toList)
  }

  def zmqPublish(addr: String, result: DataStream[List[String]]) = {
    result.addSink(new ZmqPublisher(addr)).setParallelism(1)
  }*/
}

class TextLoader[T <: SchemaClass :ClassTag: TypeInformation] extends java.io.Serializable {
  def loadStream(env: StreamExecutionEnvironment, path: String, extract: (Array[String]) => T): DataStream[T] =
    env.readTextFile(path).map(line => extract(Array(line)))
}

object TextLoader extends java.io.Serializable {
  def apply[T <: SchemaClass :ClassTag: TypeInformation](): TextLoader[T] = {
    new TextLoader[T]
  }
}

object PigStream {
  def apply[T <: SchemaClass :ClassTag: TypeInformation](): PigStream[T] = {
    new PigStream
  }
}
/*
class RDFStream extends java.io.Serializable {

  val pattern = "([^\"]\\S*|\".+?\")\\s*".r

  def rdfize(line: String): Array[String] = {
    val fields = pattern.findAllIn(line).map(_.trim)
    fields.toArray.slice(0, 3)
  }

  def load(env: StreamExecutionEnvironment, path: String): DataStream[Array[String]] = {
    env.readTextFile(path).map(line => rdfize(line))
  }

  def connect(env: StreamExecutionEnvironment, host: String, port: Int): DataStream[Array[String]] = {
    env.socketTextStream(host,port).map(line => rdfize(line))
  }

 def zmqSubscribe(env: StreamExecutionEnvironment, addr: String): DataStream[Array[String]] = {
    env.addSource(new ZmqSubscriber(addr)).map(line => rdfize(line))
  }
}

object RDFStream {
  def apply(): RDFStream = {
    new RDFStream
  }
}*/
