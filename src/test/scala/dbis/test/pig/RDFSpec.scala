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

import dbis.pig.op.{PositionalField, NamedField, Value, TriplePattern}
import dbis.pig.plan.rewriting.RDF
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class RDFSpec extends FlatSpec with Matchers with TableDrivenPropertyChecks {
  "TriplePatterns" should "be convertible to strings" in {
    val patterns = Table(
      ("patterns", "string"),
      (List(TriplePattern(NamedField("a"), Value("\"firstName\""), Value("\"Stefan\""))),
        """{ ?a "firstName" "Stefan" }"""),
      (List(TriplePattern(PositionalField(0), Value("\"firstName\""), Value("\"Stefan\""))),
        """{ $0 "firstName" "Stefan" }"""),
      (List(
        TriplePattern(PositionalField(0), Value("\"firstName\""), Value("\"Stefan\"")),
        TriplePattern(PositionalField(0), Value("\"lastName\""), Value("\"Hage\""))),
        """{ $0 "firstName" "Stefan" . $0 "lastName" "Hage" }"""
        )
    )

    forAll(patterns) {(patterns: List[TriplePattern], bgpstring: String) =>
      RDF.triplePatternsToString(patterns) shouldBe bgpstring
    }
  }
}
