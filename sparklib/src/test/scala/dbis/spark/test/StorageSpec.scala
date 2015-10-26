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

package dbis.pig.backends.spark.test

import java.io.File

import dbis.pig.backends.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import org.apache.commons.io.FileUtils

case class Person(name: String, age: Int) extends java.io.Serializable with SchemaClass {
  override def mkString(delim: String) = s"${name}${delim}${age}"
}

case class DataRecord(col1: Int, col2: String) extends java.io.Serializable with SchemaClass {
  override def mkString(delim: String) = s"${col1}${delim}${col2}"
}

class StorageSpec extends FlatSpec with Matchers with BeforeAndAfter {
  var sc: SparkContext = _
  var conf: SparkConf = _

  before {
    // to avoid Akka rebinding to the same port, since it doesn't unbind
    // immediately after shutdown
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    conf = new SparkConf().setMaster("local").setAppName(getClass.getSimpleName)
    sc = new SparkContext(conf)
  }

  after {
    // cleanup SparkContext data
    sc.stop()
    sc = null
    conf = null
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  "PigStorage" should "load objects using an extractor" in {
    val res = PigStorage[Person]().load(sc, "sparklib/src/test/resources/person.csv",
      (data: Array[String]) => Person(data(0), data(1).toInt), ",")
    res.collect() should be (Array(Person("Anna", 21), Person("John", 53), Person("Mike", 32)))
  }

  it should "save and load records" in {
    val res = PigStorage[Person]().load(sc, "sparklib/src/test/resources/person.csv",
      (data: Array[String]) => Person(data(0), data(1).toInt), ",")
    PigStorage[Person]().write("person.data", res, "|")
    val otherRes = PigStorage[Person]().load(sc, "person.data",
      (data: Array[String]) => Person(data(0), data(1).toInt), "[|]")
    res.collect() should be (otherRes.collect())
    FileUtils.deleteDirectory(new File("person.data"))
   }

  it should "load simple text lines" in {
    val extractor = (data: Array[String]) => TextLine(data(0))

    val res = PigStorage[TextLine]().load(sc, "sparklib/src/test/resources/person.csv",
      (data: Array[String]) => TextLine(data(0)))
    res.collect() should be (Array(TextLine("Anna,21"), TextLine("John,53"), TextLine("Mike,32")))
  }

  it should "load simple CSV records" in {
    val res = PigStorage[Record]().load(sc, "sparklib/src/test/resources/person.csv",
      (data: Array[String]) => Record(data), ",")
    val resArray = res.collect()
    resArray.length should be (3)
    resArray(0).fields should be (Array("Anna", "21"))
    resArray(1).fields should be (Array("John", "53"))
    resArray(2).fields should be (Array("Mike", "32"))
  }

  "BinStorage" should "save and load a RDD" in {
    val data = PigStorage[Person]().load(sc, "sparklib/src/test/resources/person.csv",
      (data: Array[String]) => Person(data(0), data(1).toInt), ",")
    BinStorage[Person]().write("person.ser", data)

    val otherData = BinStorage[Person]().load(sc, "person.ser",
      (data: Array[String]) => Person(data(0), data(1).toInt))
    otherData.collect().length should be (3)
    data.collect() should be (otherData.collect())
    FileUtils.deleteDirectory(new File("person.ser"))
  }

  "JDBCStorage" should "load data from a H2 database" in {
    val data = JdbcStorage[DataRecord]().load(sc, "data", "org.h2.Driver",
      "jdbc:h2:file:./src/it/resources/input/test?user=sa",
      (row: org.apache.spark.sql.Row) => DataRecord(row.getInt(0), row.getString(1)))
    data.collect() should be (Array(DataRecord(1, "One"), DataRecord(2, "Two"), DataRecord(3, "Three"),
      DataRecord(4, "Four"), DataRecord(5, "Five"), DataRecord(6, "Six")))
  }
}