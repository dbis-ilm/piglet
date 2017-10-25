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

package dbis.piglet.backends.flink.test

import dbis.piglet.backends.SchemaClass
import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalatest._

case class Person(name: String, age: Int) extends java.io.Serializable with SchemaClass {
  override def mkString(delim: String) = s"$name$delim$age"
}

case class DataRecord(col1: Int, col2: String) extends java.io.Serializable with SchemaClass {
  override def mkString(delim: String) = s"$col1$delim$col2"
}

case class DoubleRecord(col1: Double, col2: Double) extends java.io.Serializable with SchemaClass {
  override def mkString(delim: String) = s"$col1$delim$col2"
}

class StorageSpec extends FlatSpec with Matchers with BeforeAndAfter {
  var env: ExecutionEnvironment = _

  before {
    env = ExecutionEnvironment.getExecutionEnvironment
  }

  after {
    // cleanup
    env = null
  }
  /*
  "PigStorage" should "load objects using an extractor" in {
    val res = PigStorage[Person]().load(env, "sparklib/src/test/resources/person.csv",
      (data: Array[String]) => Person(data(0), data(1).toInt), ",")
    val result = res.collect()
    env.execute()
    result should be (Array(Person("Anna", 21), Person("John", 53), Person("Mike", 32)))
  }

  it should "save and load records" in {
    val res = PigStorage[Person]().load(env, "sparklib/src/test/resources/person.csv",
      (data: Array[String]) => Person(data(0), data(1).toInt), ",")
    PigStorage[Person]().write("person.data", res, "|")
    val otherRes = PigStorage[Person]().load(env, "person.data",
      (data: Array[String]) => Person(data(0), data(1).toInt), "[|]")
    env.execute()
    res.collect() should be (otherRes.collect())
    FileUtils.deleteDirectory(new File("person.data"))
  }

   it should "load simple CSV records" in {
    val res = PigStorage[Record]().load(env, "sparklib/src/test/resources/person.csv",
      (data: Array[String]) => Record(data), ",")
    env.execute()
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

  "BinStorage" should "save and load a RDD with doubles" in {
    val data = PigStorage[DoubleRecord]().load(sc, "sparklib/src/test/resources/values.csv",
      (data: Array[String]) => DoubleRecord(data(0).toDouble, data(1).toDouble), ",")
    BinStorage[DoubleRecord]().write("values.ser", data)

    val otherData = BinStorage[DoubleRecord]().load(sc, "values.ser",
      (data: Array[String]) => DoubleRecord(data(0).toDouble, data(1).toDouble))
    otherData.collect().length should be (3)
    data.collect() should be (otherData.collect())
    FileUtils.deleteDirectory(new File("values.ser"))
  }

  "JDBCStorage" should "load data from a H2 database" in {
    val data = JdbcStorage[DataRecord]().load(sc, "data",
      (row: org.apache.spark.sql.Row) => DataRecord(row.getInt(0), row.getString(1)), "org.h2.Driver",
      "jdbc:h2:file:./src/it/resources/input/test?user=sa")
    data.collect() should be (Array(DataRecord(1, "One"), DataRecord(2, "Two"), DataRecord(3, "Three"),
      DataRecord(4, "Four"), DataRecord(5, "Five"), DataRecord(6, "Six")))
  }

  "RDFStorage" should "load RDF data from NTriple file" in {
    val res = RDFFileStorage[Record]().load(sc, "sparklib/src/test/resources/person.csv",
      (data: Array[String]) => Record(data))
    val resArray = res.collect()
  }
  */
}