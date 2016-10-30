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

package dbis.cep.test.spark

import java.io.File

import dbis.piglet.backends.{ SchemaClass, Record }
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest._
import org.apache.commons.io.FileUtils
import dbis.piglet.cep.nfa._
import dbis.piglet.cep.ops.SelectionStrategy._
import dbis.piglet.cep.ops.OutputStrategy._
import dbis.piglet.cep.spark.CustomRDDMatcher._
import scala.collection.mutable.ListBuffer

import scala.collection.mutable.HashMap

case class BatchDoubleRecord(col1: Int, col2: Int) extends java.io.Serializable with SchemaClass {
  override def mkString(delim: String) = s"${col1}${delim}${col2}"
}

object OurBatchNFA {
    def filter1(record: BatchDoubleRecord, rvalues: NFAStructure[BatchDoubleRecord]): Boolean = record.col1 == 1
    def filter2(record: BatchDoubleRecord, rvalues: NFAStructure[BatchDoubleRecord]): Boolean = record.col1 == 2
    def filter3(record: BatchDoubleRecord, rvalues: NFAStructure[BatchDoubleRecord]): Boolean = record.col1 == 3
    def createNFA = {
      val testNFA: NFAController[BatchDoubleRecord] = new NFAController()
      val firstState = testNFA.createAndGetStartState("First")
      val secondState = testNFA.createAndGetNormalState("Second")
      val thirdState = testNFA.createAndGetNormalState("Third")
      val finalState = testNFA.createAndGetFinalState("Final")

      val firstEdge = testNFA.createAndGetForwardEdge(filter1)
      val secondEdge = testNFA.createAndGetForwardEdge(filter2)
      val thirdEdge = testNFA.createAndGetForwardEdge(filter3)

      testNFA.createForwardTransition(firstState, firstEdge, secondState)
      testNFA.createForwardTransition(secondState, secondEdge, thirdState)
      testNFA.createForwardTransition(thirdState, thirdEdge, finalState)
      testNFA
    }
  }

object OurReferBatchNFA {
    def filter1(record: BatchDoubleRecord, rvalues: NFAStructure[BatchDoubleRecord]): Boolean = record.col1 == 1
    def filter2(record: BatchDoubleRecord, rvalues: NFAStructure[BatchDoubleRecord]): Boolean = record.col1 == rvalues.events(0).col1 + 1
    def filter3(record: BatchDoubleRecord, rvalues: NFAStructure[BatchDoubleRecord]): Boolean = record.col1 == rvalues.events(1).col1 + 1
    def createNFA = {
      val testNFA: NFAController[BatchDoubleRecord] = new NFAController()
      val firstState = testNFA.createAndGetStartState("First")
      val secondState = testNFA.createAndGetNormalState("Second")
      val thirdState = testNFA.createAndGetNormalState("Third")
      val finalState = testNFA.createAndGetFinalState("Final")

      val firstEdge = testNFA.createAndGetForwardEdge(filter1)
      val secondEdge = testNFA.createAndGetForwardEdge(filter2)
      val thirdEdge = testNFA.createAndGetForwardEdge(filter3)

      testNFA.createForwardTransition(firstState, firstEdge, secondState)
      testNFA.createForwardTransition(secondState, secondEdge, thirdState)
      testNFA.createForwardTransition(thirdState, thirdEdge, finalState)
      testNFA
    }
  }

class SparkBatchCEPTest extends FlatSpec with Matchers with BeforeAndAfter {
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
  val sample = Seq(
    BatchDoubleRecord(1, 1),
    BatchDoubleRecord(2, 2),
    BatchDoubleRecord(1, 3),
    BatchDoubleRecord(2, 4),
    BatchDoubleRecord(3, 5),
    BatchDoubleRecord(1, 6),
    BatchDoubleRecord(2, 7),
    BatchDoubleRecord(3, 8));

  "Spark CEP" should "detect the pattern SEQ(A, B, C) with first match" in {
    val data = sc.makeRDD(sample, 1)
    val res = data.matchNFA(OurBatchNFA.createNFA, FirstMatch)
    val result = res.collect()
    result should be(Array(
      BatchDoubleRecord(1, 1),
      BatchDoubleRecord(2, 2),
      BatchDoubleRecord(3, 5),
      BatchDoubleRecord(1, 6),
      BatchDoubleRecord(2, 7),
      BatchDoubleRecord(3, 8)))
  }

  it should "detect the pattern SEQ(A, B, C) with any match" in {
    val data = sc.makeRDD(sample, 1)
    val res = data.matchNFA(OurBatchNFA.createNFA, AllMatches)
    val result = res.collect()
    result should be(Array(
      BatchDoubleRecord(1, 3),
      BatchDoubleRecord(2, 4),
      BatchDoubleRecord(3, 5),
      BatchDoubleRecord(1, 6),
      BatchDoubleRecord(2, 7),
      BatchDoubleRecord(3, 8)))
  }

  it should "detect the pattern SEQ(A, B, C) with next match" in {
    val data = sc.makeRDD(sample, 1)
    val res = data.matchNFA(OurBatchNFA.createNFA, NextMatches)
    val result = res.collect()
    result should be(Array(BatchDoubleRecord(1, 3),
      BatchDoubleRecord(2, 4),
      BatchDoubleRecord(3, 5),
      BatchDoubleRecord(1, 1),
      BatchDoubleRecord(2, 2),
      BatchDoubleRecord(3, 5),
      BatchDoubleRecord(1, 6),
      BatchDoubleRecord(2, 7),
      BatchDoubleRecord(3, 8)))
  }

  it should "detect the pattern SEQ(A, B, C) with contiguity match" in {
    val data = sc.makeRDD(sample, 1)
    val res = data.matchNFA(OurBatchNFA.createNFA, ContiguityMatches)
    val result = res.collect()
    result should be(Array(BatchDoubleRecord(1, 3),
      BatchDoubleRecord(2, 4),
      BatchDoubleRecord(3, 5),
      BatchDoubleRecord(1, 6),
      BatchDoubleRecord(2, 7),
      BatchDoubleRecord(3, 8)))
  }
  
  "Spark CEP" should "detect the pattern SEQ(A, B, C) with first match and related value" in {
    val data = sc.makeRDD(sample, 1)
    val res = data.matchNFA(OurReferBatchNFA.createNFA, FirstMatch)
    val result = res.collect()
    result should be(Array(
      BatchDoubleRecord(1, 1),
      BatchDoubleRecord(2, 2),
      BatchDoubleRecord(3, 5),
      BatchDoubleRecord(1, 6),
      BatchDoubleRecord(2, 7),
      BatchDoubleRecord(3, 8)))
  }
}
