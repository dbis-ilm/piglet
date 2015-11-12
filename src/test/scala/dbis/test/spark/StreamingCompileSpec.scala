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
package dbis.test.spark

import dbis.pig.backends.BackendManager
import dbis.pig.codegen.BatchCodeGen
import dbis.pig.codegen.StreamingCodeGen
import dbis.pig.parser.LanguageFeature
import dbis.pig.parser.PigParser.parseScript
import dbis.pig.backends.BackendManager
import dbis.pig.codegen.{StreamingCodeGen, BatchCodeGen}
import dbis.pig.op._
import dbis.pig.plan.DataflowPlan
import dbis.pig.plan.rewriting.Rewriter._
import dbis.pig.plan.rewriting.Rules
import dbis.pig.plan.rewriting.Rules
import dbis.pig.schema._
import dbis.pig.udf.UDFTable
import dbis.test.TestTools._
import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}

class StreamingCompileSpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  override def beforeAll() {
    Rules.registerAllRules()
  }

  def cleanString(s: String): String = s.stripLineEnd.replaceAll( """\s+""", " ").trim

  val backendConf = BackendManager.backend("sparks")
  BackendManager.backend = backendConf
  val templateFile = backendConf.templateFile

  "The compiler output" should "contain the Spark Streaming header & footer" in {
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitImport
      + codeGenerator.emitHeader1("test")
      + codeGenerator.emitHeader2("test")
      + codeGenerator.emitFooter)
    val expectedCode = cleanString(
      """
        |import org.apache.spark._
        |import org.apache.spark.streaming._
        |import dbis.pig.backends.spark._
        |
        |object SECONDS {
        |  def apply(p: Long) = Seconds(p)
        |}
        |object MINUTES {
        |  def apply(p: Long) = Minutes(p)
        |}
        |
        |object test {
        |    def main(args: Array[String]) {
        |      val conf = new SparkConf().setAppName("test_App")
        |      val ssc = new StreamingContext(conf, Seconds(1))
        |      ssc.start()
        |      ssc.awaitTermination()
        |   }
        |}
      """.stripMargin)
    assert(generatedCode == expectedCode)
  }

  it should "contain conde for receiving a stream" in {
    val ops = parseScript(
      """
        |in = SOCKET_READ 'localhost:5555' USING PigStream(',') AS (s1: chararray, s2: int);
        |DUMP in;
      """.stripMargin, LanguageFeature.StreamingPig)

    val plan = new DataflowPlan(ops)
    val rewrittenPlan = processPlan(plan)
    val codeGenerator = new StreamingCodeGen(templateFile)
    val generatedCode = cleanString(codeGenerator.emitNode(rewrittenPlan.findOperatorForAlias("in").get))
    val expectedCode = cleanString(
    """
      |val in = PigStream[_t1_Tuple].receiveStream(ssc, "localhost", 5555, ???, ",")
    """.stripMargin
    )
    println("--> " + generatedCode)
  }
}