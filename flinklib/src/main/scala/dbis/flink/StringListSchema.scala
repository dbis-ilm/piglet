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

class StringListSchema extends DeserializationSchema[List[String]] with SerializationSchema[List[String], Array[Byte]] {

  override def deserialize(message: Array[Byte]): List[String] = {
    SerializationUtils.deserialize[Array[String]](message).toList
  }   

  override def isEndOfStream(nextElement: List[String]): Boolean = {
    false
  }   

  override def serialize(element: List[String]): Array[Byte] = {
    SerializationUtils.serialize(element.toArray)
  }   

  override def getProducedType(): TypeInformation[List[String]] = {
    TypeExtractor.getForClass(classOf[List[String]])
  }   
}

