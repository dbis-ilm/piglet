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
package dbis.piglet.udf

import dbis.piglet.schema._

import scala.collection.mutable.ListBuffer

case class UDF(name: String, scalaName: String, paramTypes: List[PigType], resultType: PigType, isAggregate: Boolean) {
  def numParams = paramTypes.size
}

object UDFTable {
  lazy val funcTable = ListBuffer[UDF](
    UDF("COUNT", "PigFuncs.count", List(Types.AnyType), Types.LongType, isAggregate = true),
    UDF("AVG", "PigFuncs.average", List(Types.IntType), Types.DoubleType, isAggregate = true),
    UDF("AVG", "PigFuncs.average", List(Types.LongType), Types.DoubleType, isAggregate = true),
    UDF("AVG", "PigFuncs.average", List(Types.FloatType), Types.DoubleType, isAggregate = true),
    UDF("AVG", "PigFuncs.average", List(Types.DoubleType), Types.DoubleType, isAggregate = true),
    UDF("SUM", "PigFuncs.sum", List(Types.IntType), Types.IntType, isAggregate = true),
    UDF("SUM", "PigFuncs.sum", List(Types.LongType), Types.LongType, isAggregate = true),
    UDF("SUM", "PigFuncs.sum", List(Types.FloatType), Types.FloatType, isAggregate = true),
    UDF("SUM", "PigFuncs.sum", List(Types.DoubleType), Types.DoubleType, isAggregate = true),
    UDF("MIN", "PigFuncs.min", List(Types.IntType), Types.IntType, isAggregate = true),
    UDF("MIN", "PigFuncs.min", List(Types.LongType), Types.LongType, isAggregate = true),
    UDF("MIN", "PigFuncs.min", List(Types.FloatType), Types.FloatType, isAggregate = true),
    UDF("MIN", "PigFuncs.min", List(Types.DoubleType), Types.DoubleType, isAggregate = true),
    UDF("MAX", "PigFuncs.max", List(Types.IntType), Types.IntType, isAggregate = true),
    UDF("MAX", "PigFuncs.max", List(Types.LongType), Types.LongType, isAggregate = true),
    UDF("MAX", "PigFuncs.max", List(Types.FloatType), Types.FloatType, isAggregate = true),
    UDF("MAX", "PigFuncs.max", List(Types.DoubleType), Types.DoubleType, isAggregate = true),
    UDF("TOKENIZE", "PigFuncs.tokenize", List(Types.CharArrayType), BagType(TupleType(Array(Field("", Types.ByteArrayType)))), isAggregate = false),
    UDF("TOMAP", "PigFuncs.toMap", List(Types.AnyType), MapType(Types.ByteArrayType), isAggregate = false),
    UDF("STARTSWITH","PigFuncs.startswith", List(Types.CharArrayType, Types.CharArrayType), Types.BooleanType, isAggregate = false),
    UDF("ENDSWITH","PigFuncs.endswith", List(Types.CharArrayType, Types.CharArrayType), Types.BooleanType, isAggregate = false),
    UDF("STRLEN", "PigFuncs.strlen", List(Types.CharArrayType), Types.IntType,isAggregate = false),
    UDF("LOWER", "PigFuncs.lowercase", List(Types.CharArrayType), Types.CharArrayType, isAggregate = false),
    UDF("UPPER", "PigFuncs.uppercase", List(Types.CharArrayType), Types.CharArrayType, isAggregate = false),
    UDF("CONCAT", "PigFuncs.concat", List(Types.CharArrayType, Types.CharArrayType), Types.CharArrayType, isAggregate = false),
    UDF("CONTAINS", "PigFuncs.contains", List(Types.CharArrayType, Types.CharArrayType), Types.BooleanType, isAggregate = false),
    UDF("TODOUBLE", "PigFuncs.toDouble", List(Types.CharArrayType), Types.DoubleType, isAggregate = false),
    UDF("SQRT", "math.sqrt", List(Types.DoubleType), Types.DoubleType, isAggregate = false),
    UDF("POW", "math.pow", List(Types.DoubleType, Types.DoubleType), Types.DoubleType, isAggregate = false),
    UDF("S_DISTANCE", "dbis.stark.Distance.seuclid", List(Types.AnyType, Types.AnyType), Types.DoubleType, isAggregate = false)
  )

  def addUDF(func: UDF): Unit = {
    funcTable += func
  }

  /**
   * Checks whether two parameter types are the same.
   *
   * @param funcType the parameter type of the function
   * @param paramType the current parameter type
   * @return true if the same type, otherwise false
   */
  def typeMatch(funcType: PigType, paramType: PigType): Boolean = if (funcType == Types.AnyType) true else funcType == paramType


  /**
   * Checks whether two lists of parameter types are equal.
   *
   * @param funcTypes the list of parameter type of the function
   * @param paramTypes the current list of parameter type
   * @return true if the parameters are of the same types, otherwise false
   */
  def typeListMatch(funcTypes: List[PigType], paramTypes: List[PigType]): Boolean = {
    val matches = funcTypes.zip(paramTypes).map{case (t1, t2) => typeMatch(t1, t2)}
    matches.size == funcTypes.size && ! matches.contains(false)
  }

  /**
   * Checks whether two lists of parameter types contain compatible types.
   *
   * @param funcTypes the list of parameter type of the function
   * @param paramTypes the current list of parameter type
   * @return true if the parameters types are compatible, otherwise false
   */
  def typeListCompatibility(funcTypes: List[PigType], paramTypes: List[PigType]): Boolean = {
    val matches = funcTypes.zip(paramTypes).map{case (t1, t2) => Types.typeCompatibility(t1, t2)}
    matches.size == funcTypes.size && ! matches.contains(false)
  }

  /**
   * Try to find a UDF with the given name and a matching parameter type.
   *
   * @param name the name of the UDF
   * @param paramType the parameter type
   * @return the UDF object
   */
  def findUDF(name: String, paramType: PigType): Option[UDF] = {
    // 1st, we check candidates with the same name
    val candidates = funcTable.filter(udf => udf.name == name.toUpperCase && udf.numParams == 1)
    // if we find a udf among these candidates with the same number and type of parameter, then we return it directly
    val res = candidates.filter{udf: UDF => typeMatch(udf.paramTypes.head, paramType)}
    if (res.nonEmpty)
      res.headOption
    else {
      // otherwise we check for a udf with type compatible parameter
      candidates.find { udf: UDF => Types.typeCompatibility(udf.paramTypes.head, paramType) }
    }
  }

  /**
   * Try to find a UDF with the given name and a matching parameter type.
   *
   * @param name the name of the UDF
   * @param paramTypes the list of parameter types
   * @return the UDF object
   */
  def findUDF(name: String, paramTypes: List[PigType]): Option[UDF] = {
    // 1st, we check candidates with the same name (not the same number of parameters in order to allow AnyType)
    val candidates = funcTable.filter(udf => udf.name == name.toUpperCase /* && udf.numParams == paramTypes.size*/)
    // if we find a udf among these candidates with the same number and type of parameters, then we return it directly
    val res = candidates.filter { udf: UDF => typeListMatch(udf.paramTypes, paramTypes) }
    if (res.nonEmpty)
      res.headOption
    else {
      // otherwise we check for a udf with type compatible parameter
      candidates.find { udf: UDF => typeListCompatibility(udf.paramTypes, paramTypes) }
    }
  }

  /**
    * Try to find a UDF with the given name and return the first of the list.
    *
    * @param name the name of the UDF
    * @return the UDF object
    */
  def findFirstUDF(name: String): Option[UDF] = {
    val res = funcTable.filter{ udf: UDF => udf.name == name }
    res.headOption
  }
}
