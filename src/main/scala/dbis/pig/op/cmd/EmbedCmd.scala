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
import dbis.pig.udf.{ScalaUDFParser, UDFTable, UDF}
import scala.collection.mutable.ListBuffer
import dbis.pig.op.PigOperator

import scala.util.parsing.input.CharSequenceReader


/** Wraps code that is to be embedded in the compiled Scala application.
 *
 * @param code The embedded code.
 */
case class EmbedCmd(code: String, ruleCode: Option[String]) extends PigOperator(List(), List()) {

  def this(code: String) = this(code, None)

  /**
   * Analyze the Scala code of an embedded section for Scala functions
   * and try to collect them as UDFs.
   *
   * @return the list of identified UDFs
   */
  def extractUDFs(): List[UDF] = {
    val udfs = ListBuffer[UDF]()
    val pattern = "def\\s*\\w*\\s*\\(([^\\)]*)\\)\\s*:\\s*[^=]*\\s*=".r
    val funcs = pattern.findAllIn(code.replaceAll("(\r\n)|\r|\n", ""))
    funcs.foreach(s => {
      val parser = new ScalaUDFParser()
      val udf = parser.parseDef(new CharSequenceReader(s))
      udfs += udf
      UDFTable.addUDF(udf)
    })
    udfs.toList
  }


}

