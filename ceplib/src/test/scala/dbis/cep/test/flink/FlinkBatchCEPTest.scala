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

package dbis.cep.test.flink

import java.io.File

import dbis.pig.backends.{ Record, SchemaClass }
import org.apache.flink.api.scala.ExecutionEnvironment
import org.scalatest._
import org.apache.commons.io.FileUtils
import org.apache.flink.api.scala._
import dbis.pig.cep.nfa._
import dbis.pig.cep.ops.SelectionStrategy._
import dbis.pig.cep.ops.OutputStrategy._
import dbis.pig.cep.flink.CustomDataSetMatcher._
// import dbis.flink.test.FlinkBatchTestInit
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

class FlinkBatchCEPTest extends FlatSpec with Matchers {

  val sample = Seq(
      BatchDoubleRecord(1,1), 
      BatchDoubleRecord(2,2), 
      BatchDoubleRecord(1,3), 
      BatchDoubleRecord(2,4), 
      BatchDoubleRecord(3,5), 
      BatchDoubleRecord(1,6),
      BatchDoubleRecord(2,7),
      BatchDoubleRecord(3,8))
  
  "Flink CEP" should "detect the pattern SEQ(A, B, C) with first match" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromCollection(sample)
    val res = data.matchNFA(OurBatchNFA.createNFA, FirstMatch)
    val result = res.collect()
    result should be (Array(
        BatchDoubleRecord(1,1), 
        BatchDoubleRecord(2,2), 
        BatchDoubleRecord(3,5), 
        BatchDoubleRecord(1,6), 
        BatchDoubleRecord(2,7), 
        BatchDoubleRecord(3,8)))
  }

  it should "detect the pattern SEQ(A, B, C) with any match" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromCollection(sample)
    val res = data.matchNFA(OurBatchNFA.createNFA, AllMatches)
    val result = res.collect()
    result should be (Array(
        BatchDoubleRecord(1,3), 
        BatchDoubleRecord(2,4), 
        BatchDoubleRecord(3,5), 
        BatchDoubleRecord(1,6), 
        BatchDoubleRecord(2,7), 
        BatchDoubleRecord(3,8)))
  }

  it should "detect the pattern SEQ(A, B, C) with next match" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromCollection(sample)
    val res = data.matchNFA(OurBatchNFA.createNFA, NextMatches)
    val result = res.collect()
    result should be (Array(BatchDoubleRecord(1,3), 
        BatchDoubleRecord(2,4), 
        BatchDoubleRecord(3,5),
        BatchDoubleRecord(1,1),
        BatchDoubleRecord(2,2),
        BatchDoubleRecord(3,5),
        BatchDoubleRecord(1,6),
        BatchDoubleRecord(2,7),
        BatchDoubleRecord(3,8)))
  }

  it should "detect the pattern SEQ(A, B, C) with contiguity match" in {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromCollection(sample)
    val res = data.matchNFA(OurBatchNFA.createNFA, ContiguityMatches)
    val result = res.collect()
    result should be (Array(BatchDoubleRecord(1,3),
        BatchDoubleRecord(2,4),
        BatchDoubleRecord(3,5),
        BatchDoubleRecord(1,6),
        BatchDoubleRecord(2,7),
        BatchDoubleRecord(3,8)))
  }
}

