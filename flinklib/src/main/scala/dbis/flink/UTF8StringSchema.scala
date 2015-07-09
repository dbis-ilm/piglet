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

import org.apache.commons.lang3.SerializationUtils
import org.apache.flink.streaming.util.serialization._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor

class UTF8StringSchema extends DeserializationSchema[String] with SerializationSchema[List[String], Array[Byte]] {

  override def deserialize(message: Array[Byte]): String = {
    new String(message, "UTF-8")
  }   

  override def isEndOfStream(nextElement: String): Boolean = {
    false
  }   

  override def serialize(element: List[String]): Array[Byte] = {
    element.mkString(",").getBytes("UTF-8")
  }   

  override def getProducedType(): TypeInformation[String] = {
    TypeExtractor.getForClass(classOf[String])
  }   
}

