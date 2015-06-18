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
package dbis.test.op

import dbis.pig.op.{Limit, Store}
import org.scalatest.{FlatSpec, Matchers}

class PigOperatorSpec extends FlatSpec with Matchers {
  "An operator" should "allow updating its output relation name if it has no outputs" in {
    val op = Limit("b", "a", 5)
    op.output = Some("b")
    op.output = Some("d")
  }

  it should "not allow updating its output relation name if not all outputs read from the new name" in {
    val op = Limit("b", "a", 5)
    val op2 = Limit("c", "b", 3)
    op.output = Some("b")
    op.outputs = List(op2)
    intercept[IllegalArgumentException] {
      op.output = Some("d")
    }
  }

  it should "not allow updating its outputs if it doesn't return a relation" in {
    val op = Store("a", "foo.csv")
    val op2 = Limit("a", "b", 5)
    intercept[IllegalStateException] {
      op.outputs = List(op2)
    }
  }

  it should "allow updating its outputs if the new ones read from it" in {
    val op = Limit("b", "a", 5)
    op.output = Some("b")
    val op2 = Limit("c", "b", 3)
    op.outputs = List(op2)
  }

  it should "allow unsetting its outputs" in {
    val op = Limit("b", "a", 5)
    op.output = Some("b")
    val op2 = Limit("c", "b", 3)
    op.outputs = List(op2)
    op.outputs = List()
  }

}
