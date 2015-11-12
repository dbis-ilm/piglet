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

package dbis.pig.op.cmd

import dbis.pig.schema.{Types, PigType}
import dbis.pig.udf.{UDFTable, UDF}
import scala.collection.mutable.ListBuffer
import dbis.pig.op.PigOperator


/** Wraps code that is to be embedded in the compiled Scala application.
 *
 * @param code The embedded code.
 */
case class EmbedCmd(code: String, ruleCode: Option[String]) extends PigOperator {
  def this(code: String) = this(code, None)

  /**
   * Analyze the Scala code of an embedded section for Scala functions
   * and try to collect them as UDFs.
   *
   * @return the list of identified UDFs
   */
  def extractUDFs(): List[UDF] = {
    def stringToPigType(s: String): PigType = s match {
      case "String" => Types.CharArrayType
      case "Int" => Types.IntType
      case "Double" => Types.DoubleType
      case "Float" => Types.FloatType
      case "Long" => Types.LongType
      case "Boolean" => Types.BooleanType
      case _ => Types.ByteArrayType
    }

    val udfs = ListBuffer[UDF]()
    val pattern = "def\\s*\\w*\\s*\\(([^\\)]*)\\)\\s*:\\s*\\w*\\s*=".r
    val namePattern = "def\\s*\\w*\\s*".r
    val typePattern = "\\)\\s*:\\s*\\w*\\s*=$".r
    val paramPattern = "\\(.*\\)".r
    val nonParamPattern = "\\(\\s*\\)".r
    val funcs = pattern.findAllIn(code.replaceAll("(\r\n)|\r|\n", ""))
    funcs.foreach(s => {
      val nameStr = namePattern.findFirstIn(s).get.split(" ")(1)
      val s2 = typePattern.findFirstIn(s).get
      val p1 = s2.indexOf(":")
      val p2 = s2.lastIndexOf("=")
      val typeStr = s2.substring(p1 + 1, p2 - p1).trim
      // TODO: handle more complex parameter types such as tuple, bag, and map
      val paramStr = paramPattern.findFirstIn(s).get
      val nonParamStr = nonParamPattern.findFirstIn(paramStr)
      val numParams = if (nonParamStr.isDefined) 0 else paramStr.count(c => c == ',') + 1
      // println("name = '" + nameStr + "', typeStr = '" + typeStr + "', numParams = " + numParams)
      val params = ListBuffer[PigType]()
      for (i <- 1 to numParams) params += Types.AnyType
      val udf = UDF(nameStr.toUpperCase, nameStr, params.toList, stringToPigType(typeStr), false)
      // println("add udf: " + udf)
      udfs += udf
      UDFTable.addUDF(udf)
    })
    udfs.toList
  }


}

