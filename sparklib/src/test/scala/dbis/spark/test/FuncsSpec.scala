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

package dbis.piglet.backends.spark.test

import dbis.piglet.backends.spark.PigFuncs
import org.scalatest._

class FuncsSpec extends FlatSpec with Matchers {

  "The max function" should "return the maximum of a list" in {
    val l1 = List(1, 2, 3, 4, 5)
    PigFuncs.max(l1) should be (5)

    val l2 = List(5, 4, 2, 2, 1)
    PigFuncs.max(l2) should be (5)

    val l3 = List(1.0, 4.0, 5.5, 2.2, 3.1)
    PigFuncs.max(l3) should be (5.5)

    val l4 = List("AAA", "BBBB", "ZZZZ", "DDDD")
    PigFuncs.max(l4) should be ("ZZZZ")
  }

  "The min function" should "return the minimum of a list" in {
    val l1 = List(1, 2, 3, 4, 5)
    PigFuncs.min(l1) should be (1)

    val l2 = List(5, 4, 2, 2, 1)
    PigFuncs.min(l2) should be (1)

    val l3 = List(1.0, 4.0, 5.5, 2.2, 3.1)
    PigFuncs.min(l3) should be (1.0)

    val l4 = List("XXXX", "AAA", "BBBB", "ZZZZ", "DDDD")
    PigFuncs.min(l4) should be ("AAA")
  }

  "The average function" should "return the average of a list" in {
    val l1 = List(1, 2, 3, 4, 5)
    PigFuncs.average(l1) should be(3)

    val l2 = List(5, 4, 2, 2, 1)
    PigFuncs.average(l2) should be(2.8)
  }

  "The count function" should "return the number of elements" in {
    val l1 = List(1, 2, 3, 4, 5)
    PigFuncs.count(l1) should be (5)

    val l2 = List()
    PigFuncs.count(l2) should be (0)
  }

  "The sum function" should "return the sum of the elements of list" in {
    val l1 = List(1, 2, 3, 4, 5)
    PigFuncs.sum(l1) should be (15)

    val l3 = List(1.0, 4.0, 5.5, 2.2, 3.1)
    PigFuncs.sum(l3) should be (15.8 +- 1e-5)
  }

  "The tokenize function" should "split a string on ','" in {
    val s = "1,2,3,4,5,6"
    PigFuncs.tokenize(s) should be (List("1", "2", "3", "4", "5", "6"))
  }

  "The tokenize function" should "split a string on ' '" in {
    val s = "1 2 3 4 5 6"
    PigFuncs.tokenize(s) should be (List("1", "2", "3", "4", "5", "6"))
  }

  "The tokenize function" should "split a string on '&'" in {
    val s = "1&2&3&4&5&6"
    PigFuncs.tokenize(s, "&") should be (List("1", "2", "3", "4", "5", "6"))
  }

 }
