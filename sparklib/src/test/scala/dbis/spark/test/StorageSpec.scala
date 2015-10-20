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

import dbis.pig.backends.spark.{Record, TextLine, PigStorage, PigFuncs}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

case class Person(name: String, age: Int) extends java.io.Serializable

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
}