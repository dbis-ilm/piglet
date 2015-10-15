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

import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.sql._
import java.io.FileInputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.io.FileOutputStream


class PigStorage extends java.io.Serializable {
  def load(sc: SparkContext, path: String, delim: String = "\t"): RDD[List[String]] =
    sc.textFile(path).map(line => line.split(delim, -1).toList)

  def write(path: String, rdd: RDD[String]) = rdd.saveAsTextFile(path)
}

object PigStorage {
  def apply(): PigStorage = {
    new PigStorage
  }
}

//-----------------------------------------------------------------------------------------------------

class RDFFileStorage extends java.io.Serializable {
  val pattern = "([^\"]\\S*|\".+?\")\\s*".r

  def rdfize(line: String): List[String] = {
    val fields = pattern.findAllIn(line).map(_.trim)
    fields.toArray.slice(0, 3).toList
  }

  def load(sc: SparkContext, path: String): RDD[List[String]] = sc.textFile(path).map(line => rdfize(line))
}

object RDFFileStorage {
  def apply(): RDFFileStorage = {
    new RDFFileStorage
  }
}

//-----------------------------------------------------------------------------------------------------

class BinStorage extends java.io.Serializable {
  
  def load(sc: SparkContext, path: String): RDD[List[Any]] = sc.objectFile[List[Any]](path)

  def write(path: String, rdd: RDD[_]) = rdd.saveAsObjectFile(path)
}

object BinStorage {
  def apply(): BinStorage = new BinStorage
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

class JdbcStorage extends java.io.Serializable {
  def load(sc: SparkContext, table: String, driver: String, url: String): RDD[List[String]] = {
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
    // TODO: convert a DataFrame to a RDD with generic components
    sqlContext.load("jdbc", params).rdd.map(_.toSeq.map(v => v.toString).toList)
  }
}

object JdbcStorage {
  def apply(): JdbcStorage = new JdbcStorage
}
