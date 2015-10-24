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
import dbis.pig.schema._
import org.scalatest.{Matchers, FlatSpec}

class TypeSpec extends FlatSpec with Matchers {

  "The type system" should "contain predefined simple types" in {
    val t1 = Types.IntType
    t1.name should be ("int")
    t1 should be (Types.typeForName("int"))

    val t2 = Types.FloatType
    assert(t2.name == "float")
    assert(t2 == Types.typeForName("float"))

    val t3 = Types.DoubleType
    assert(t3.name == "double")
    assert(t3 == Types.typeForName("double"))

    val t4 = Types.CharArrayType
    assert(t4.name == "chararray")
    assert(t4 == Types.typeForName("chararray"))

    val t5 = Types.ByteArrayType
    assert(t5.name == "bytearray")
    assert(t5 == Types.typeForName("bytearray"))

    val t6 = Types.LongType
    assert(t6.name == "long")
    assert(t6 == Types.typeForName("long"))
  }

  it should "allow to define complex types" in {
    val t1 = MapType(Types.DoubleType)
    assert(t1.valueType == Types.typeForName("double"))

    val tt = TupleType(Array(Field("c", Types.IntType)))
    val t2 = BagType(tt)
    assert(t2.valueType == tt)

    val t3 = TupleType(Array(Field("f1", Types.IntType),
                             Field("f2", Types.DoubleType),
                             Field("f3", MapType(Types.CharArrayType))))
    assert(t3.fields(0).name == "f1")
    assert(t3.fields(1).name == "f2")
    assert(t3.fields(1).fType == Types.DoubleType)
    assert(t3.fields(2).name == "f3")
  }

  it should "check type compatibility" in {
    assert(Types.typeCompatibility(Types.IntType, Types.LongType))
    assert(Types.typeCompatibility(Types.IntType, Types.DoubleType))
    assert(Types.typeCompatibility(Types.FloatType, Types.DoubleType))
    assert(! Types.typeCompatibility(Types.IntType, Types.CharArrayType))
    assert(! Types.typeCompatibility(Types.IntType, MapType(Types.CharArrayType)))
  }

  it should "check type compatibility for bags" in {
    val t1 = BagType(TupleType(Array(Field("f1", Types.IntType),
                                              Field("f2", Types.DoubleType),
                                              Field("f3", Types.CharArrayType))))
    val t2 = BagType(TupleType(Array(Field("f1", Types.IntType),
                                              Field("f2", Types.DoubleType),
                                              Field("f3", Types.CharArrayType))))
    val t3 = BagType(TupleType(Array(Field("f11", Types.IntType),
                                              Field("f21", Types.IntType),
                                              Field("f31", Types.CharArrayType))))
    val t4 = BagType(TupleType(Array(Field("f11", Types.IntType),
                                              Field("f12", Types.DoubleType),
                                              Field("f13", Types.DoubleType),
                                              Field("f14", Types.CharArrayType))))
    assert(Types.typeCompatibility(t1, t2))
    assert(Types.typeCompatibility(t1, t3))
    assert(! Types.typeCompatibility(t1, t4))
  }

  it should "return the type description for a bag" in {
    val t = BagType(TupleType(Array(Field("f1", Types.IntType),
                                            Field("f2", Types.CharArrayType)
    )))
    t.descriptionString should be ("{f1: int, f2: chararray}")
  }

  it should "return the type description for a nested bag" in {
    val t = BagType(TupleType(Array(Field("f1", Types.IntType),
                                            Field("f2", TupleType(Array(Field("t1", Types.ByteArrayType),
                                                                Field("t2", Types.ByteArrayType)
                                              )))
    )))
    t.descriptionString should be ("{f1: int, f2: (t1: bytearray, t2: bytearray)}")
  }

  it should "return components of complex types" in {
    val tup0 = TupleType(Array(Field("t1", Types.ByteArrayType),
                               Field("t2", Types.ByteArrayType)))
    val tup = TupleType(Array(Field("f1", Types.IntType),
                              Field("f2", tup0)))
    val t = BagType(tup)
    t.typeOfComponent(0) should be (Types.IntType)
    tup.typeOfComponent("f1") should be (Types.IntType)
    tup.typeOfComponent(0) should be (Types.IntType)
    tup.typeOfComponent("f2") should be (tup0)
  }

  it should "return the correct encoding" in {
    Types.IntType.encode should be ("i")
    Types.BooleanType.encode should be ("b")
    Types.DoubleType.encode should be ("d")
    Types.LongType.encode should be ("l")
    Types.CharArrayType.encode should be ("c")
    Types.ByteArrayType.encode should be ("a")
    Types.FloatType.encode should be ("f")

    val tup0 = TupleType(Array(Field("t1", Types.ByteArrayType),
      Field("t2", Types.ByteArrayType)))
    val tup = TupleType(Array(Field("f1", Types.IntType),
      Field("f2", tup0)))
    BagType(tup).encode should be ("{(f1:if2:(t1:at2:a))}")
  }
}