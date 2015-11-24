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

package dbis.pig.backends.flink

import dbis.pig.backends._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode._

import scala.reflect.ClassTag


//-----------------------------------------------------------------------------------------------------

class PigStorage[T <: SchemaClass :ClassTag: TypeInformation] extends java.io.Serializable {
  def load(env: ExecutionEnvironment, path: String,  extract: (Array[String]) => T, delim: String = "\t"): DataSet[T] = {
    env.readTextFile(path).map(line => extract(line.split(delim, -1)))
  }

  def write(path: String, result: DataSet[T], delim: String = ",") = result.map(_.mkString(delim)).writeAsText(path).setParallelism(1)
}

object PigStorage {
  def apply[T <: SchemaClass :ClassTag: TypeInformation](): PigStorage[T] = {
    new PigStorage[T]
  }
}


class RDFFileStorage[T:ClassTag: TypeInformation] extends java.io.Serializable {
  val pattern = "([^\"]\\S*|\".+?\")\\s*".r

  def rdfize(line: String): Array[String] = {
    val fields = pattern.findAllIn(line).map(_.trim)
    fields.toArray.slice(0, 3)
  }

  def load(env: ExecutionEnvironment, path: String, extract: (Array[String]) => T): DataSet[T] =
    env.readTextFile(path).map(line => extract(rdfize(line)))
}

object RDFFileStorage {
  def apply[T:ClassTag: TypeInformation](): RDFFileStorage[T] = {
    new RDFFileStorage[T]
  }
}
