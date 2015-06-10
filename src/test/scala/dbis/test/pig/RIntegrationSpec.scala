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
package dbis.test.pig

import dbis.pig._
import dbis.pig.op._
import dbis.pig.schema._
import org.scalatest.{FlatSpec, Matchers}

import org.ddahl.jvmr.RInScala

class RIntegrationSpec extends FlatSpec with Matchers {
  "The R integration" should "allow to invoke a R script" in {
    val R = RInScala()
    R.x = Array(10.0, 20.0, 30.0)
    R.y = Array(5.0, 6.0, 7.0)
    R.eval("res <- x + y")
    val res = R.toVector[Double]("res")
    res should be (Array(15.0, 26.0, 37.0))
  }
}