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
package dbis.test.udf

import dbis.pig.schema._
import dbis.pig.udf._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.OptionValues._

class PigOperatorSpec extends FlatSpec with Matchers {
  "The UDF table" should "contain some predefined functions" in {
    val f1: UDF = UDFTable.findUDF("sum", Types.DoubleType).value
    f1.name should be ("SUM")
    f1.scalaName should be ("PigFuncs.sum")
    f1.isAggregate should be (true)
    f1.resultType should be (Types.DoubleType)

    val f2: UDF = UDFTable.findUDF("sum", Types.IntType).value
    f2.name should be ("SUM")
    f2.scalaName should be ("PigFuncs.sum")
    f2.isAggregate should be (true)
    f2.resultType should be (Types.IntType)

    val f3: UDF = UDFTable.findUDF("tokenize", Types.CharArrayType).value
    f3.name should be ("TOKENIZE")
    f3.scalaName should be ("PigFuncs.tokenize")
    f3.isAggregate should be (false)
    f3.resultType should be (BagType(TupleType(Array(Field("", Types.ByteArrayType)))))

    val f4: UDF = UDFTable.findUDF("tomap", Types.AnyType).value
    f4.name should be ("TOMAP")
    f4.scalaName should be ("PigFuncs.toMap")
    f4.isAggregate should be (false)
    f4.resultType should be (MapType(Types.ByteArrayType))
  }

  it should "allow to find type compatible functions" in {
    val f1 = UDFTable.findUDF("tomap", Types.CharArrayType).value
    f1.name should be ("TOMAP")
    f1.scalaName should be ("PigFuncs.toMap")
    f1.isAggregate should be (false)
    f1.resultType should be (MapType(Types.ByteArrayType))

    val f2 = UDFTable.findUDF("avg", Types.CharArrayType)
    f2 should be (None)

    val f3 = UDFTable.findUDF("tomap", List(Types.CharArrayType)).value
    f3.name should be ("TOMAP")
    f3.scalaName should be ("PigFuncs.toMap")
    f3.isAggregate should be (false)
    f3.resultType should be (MapType(Types.ByteArrayType))

    val f4 = UDFTable.findUDF("avg", List(Types.CharArrayType))
    f4 should be (None)

  }
}