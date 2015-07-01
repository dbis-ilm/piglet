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

import dbis.pig.schema._
import org.scalatest.FlatSpec
import dbis.pig._

class SchemaSpec extends FlatSpec {

  "The schema" should "contain f1, f2" in {
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.DoubleType),
                                                              Field("f2", Types.CharArrayType)), "t"), "s"))
    assert(schema.indexOfField("f1") == 0)
    assert(schema.indexOfField("f2") == 1)
    assert(schema.field(0) == Field("f1", Types.DoubleType))
    assert(schema.field(1) == Field("f2", Types.CharArrayType))
  }

  it should "allow to change the bag name" in {
    val schema = new Schema(BagType(TupleType(Array(Field("f1", Types.DoubleType),
      Field("f2", Types.CharArrayType)), "t"), "s"))
    schema.setBagName("bag")
    assert(schema.element.name == "bag")
  }
}
