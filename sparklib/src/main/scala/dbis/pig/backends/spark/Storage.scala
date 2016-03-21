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

package dbis.pig.backends.spark

import dbis.pig.backends._
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import java.io.FileInputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.io.FileOutputStream
import scala.tools.nsc.io._
import FileStreamReader.customFileStreamReader

import scala.reflect.ClassTag



//-----------------------------------------------------------------------------------------------------

class PigStorage[T <: SchemaClass :ClassTag] extends java.io.Serializable {
  def load(sc: SparkContext, path: String, extract: (Array[String]) => T, delim: String = "\t"): RDD[T] =
    sc.textFile(path).map(line => extract(line.split(delim, -1)))

  def write(path: String, rdd: RDD[T], delim: String = ",") = rdd.map(_.mkString(delim)).saveAsTextFile(path)
}

object PigStorage extends java.io.Serializable {
  def apply[T <: SchemaClass :ClassTag](): PigStorage[T] = {
    new PigStorage[T]
  }
}

//-----------------------------------------------------------------------------------------------------

class TextLoader[T <: SchemaClass :ClassTag] extends java.io.Serializable {
  def load(sc: SparkContext, path: String, extract: (Array[String]) => T): RDD[T] =
    sc.textFile(path).map(line => extract(Array(line)))  //.map(line => Record(Array(line)))
}

object TextLoader extends java.io.Serializable {
  def apply[T <: SchemaClass :ClassTag](): TextLoader[T] = {
    new TextLoader[T]
  }
}

//-----------------------------------------------------------------------------------------------------

class PigStream[T <: SchemaClass :ClassTag] extends java.io.Serializable {
  def receiveStream(ssc: StreamingContext, hostname: String, port: Int, extract: (Array[String]) => T, delim: String = "\t"): DStream[T] =
    ssc.socketTextStream(hostname, port).map(line => extract(line.split(delim, -1)))

  def loadStream(ssc: StreamingContext, path: String, extract: (Array[String]) => T, delim: String = "\t"): DStream[T] =
    ssc.readFile(path).map(line => extract(line.split(delim, -1)))

  def writeStream(path: String, dstream: DStream[T], delim: String = ",") = 
    dstream.foreachRDD( rdd => rdd.foreach { t => Path(path).createFile().appendAll(t.mkString(delim) + "\r\n") })

}

object PigStream extends java.io.Serializable {
  def apply[T <: SchemaClass :ClassTag](): PigStream[T] = {
    new PigStream[T]
  }
}

//-----------------------------------------------------------------------------------------------------

class RDFFileStorage[T:ClassTag] extends java.io.Serializable {
  val pattern = "([^\"]\\S*|\".+?\")\\s*".r

  def rdfize(line: String): Array[String] = {
    val fields = pattern.findAllIn(line).map(_.trim)
    fields.toArray.slice(0, 3)
  }

  def load(sc: SparkContext, path: String, extract: (Array[String]) => T): RDD[T] =
    sc.textFile(path).map(line => extract(rdfize(line)))
}

object RDFFileStorage {
  def apply[T:ClassTag](): RDFFileStorage[T] = {
    new RDFFileStorage[T]
  }
}

//-----------------------------------------------------------------------------------------------------

class BinStorage[T:ClassTag] extends java.io.Serializable {
  
  def load(sc: SparkContext, path: String, extract: (Array[String]) => T): RDD[T] = sc.objectFile[T](path)

  def write(path: String, rdd: RDD[T]) = rdd.saveAsObjectFile(path)
}

object BinStorage {
  def apply[T:ClassTag](): BinStorage[T] = new BinStorage[T]
}

//-----------------------------------------------------------------------------------------------------

class JsonStorage extends java.io.Serializable {
  def load(sc: SparkContext, path: String): RDD[List[Any]] = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // TODO: convert a DataFrame to a RDD with generic components
    sqlContext.read.json(path).rdd.map(_.toSeq.map(v => v.toString).toList)
  }
}

object JsonStorage {
  def apply(): JsonStorage = new JsonStorage
}

//-----------------------------------------------------------------------------------------------------

class JdbcStorage[T <: SchemaClass :ClassTag]  extends java.io.Serializable {
  def load(sc: SparkContext, table: String, extract: Row => T, driver: String, url: String): RDD[T] = {
    // sc.addJar("/Users/kai/Projects/h2/bin/h2-1.4.189.jar")
    var params = scala.collection.immutable.Map[String, String]()
    params += ("driver" -> driver)
    params += ("dbtable" -> table)
    val s1 = url.split("""\?""")
    params += ("url" -> s1(0))
    if (s1.length > 1) {
      val s2 = s1(1).split("&")
      s2.foreach { s =>
        val s3 = s.split("=")
        if (s3.length != 2) throw new IllegalArgumentException("invalid JDBC parameter")
        params += (s3(0) -> s3(1))
      }
    }
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // TODO: DataFrame -> RDD[T]
    sqlContext.read.format("jdbc").options(params).load().rdd.map(t => extract(t))
  }
}

object JdbcStorage {
  def apply[T <: SchemaClass :ClassTag] (): JdbcStorage[T] = new JdbcStorage[T]
}
