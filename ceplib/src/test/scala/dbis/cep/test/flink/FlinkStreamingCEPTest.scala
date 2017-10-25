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

import dbis.piglet.backends.{ Record, SchemaClass }
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest._
import org.apache.commons.io.FileUtils
import org.apache.flink.api.scala._
import dbis.piglet.cep.nfa._
import dbis.piglet.cep.ops.SelectionStrategy._
import dbis.piglet.cep.ops.OutputStrategy._
import dbis.piglet.cep.flink.CustomDataStreamMatcher._
import scala.collection.mutable.ArrayBuffer
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows

case class StreamingDoubleRecord(col1: Int, col2: Int) extends java.io.Serializable with SchemaClass {
  override def mkString(delim: String) = s"$col1$delim$col2"
}

object OurStreamingNFA {
    def filter1(record: StreamingDoubleRecord, rvalues: NFAStructure[StreamingDoubleRecord]): Boolean = record.col1 == 1
    def filter2(record: StreamingDoubleRecord, rvalues: NFAStructure[StreamingDoubleRecord]): Boolean = record.col1 == 2
    def filter3(record: StreamingDoubleRecord, rvalues: NFAStructure[StreamingDoubleRecord]): Boolean = record.col1 == 3
    def createNFA = {
      val testNFA: NFAController[StreamingDoubleRecord] = new NFAController()
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

class FlinkStreamingCEPTest extends FlatSpec with Matchers with BeforeAndAfterEach {
  var resultArray = new ArrayBuffer[StreamingDoubleRecord]
  override def beforeEach() {
     resultArray.clear()
  }

  val sample = Seq(
      StreamingDoubleRecord(1,1), 
      StreamingDoubleRecord(2,2), 
      StreamingDoubleRecord(1,3), 
      StreamingDoubleRecord(2,4), 
      StreamingDoubleRecord(3,5), 
      StreamingDoubleRecord(1,6),
      StreamingDoubleRecord(2,7),
      StreamingDoubleRecord(3,8))
      
  "Flink Streaming CEP" should "detect the pattern SEQ(A, B, C) with first match" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    val data = env.fromCollection(sample)
    val res = data.matchNFA(OurStreamingNFA.createNFA, env, FirstMatch)
  }

  it should "detect the pattern SEQ(A, B, C) with any match" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    val data = env.fromCollection(sample)
    val res = data.matchNFA(OurStreamingNFA.createNFA, env, AllMatches)
  }

  it should "detect the pattern SEQ(A, B, C) with next match" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    val data = env.fromCollection(sample)
    val res = data.matchNFA(OurStreamingNFA.createNFA, env, NextMatches)
  }

  it should "detect the pattern SEQ(A, B, C) with contiguity match" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()
    val data = env.fromCollection(sample)
    val res = data.matchNFA(OurStreamingNFA.createNFA, env, ContiguityMatches)
  }
}
