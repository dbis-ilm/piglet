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
package dbis.pig.udf

import dbis.pig.op.PigOperator
import dbis.pig.schema._

import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.parsing.input.CharSequenceReader
import scala.language.postfixOps

/**
  * Created by kai on 04.12.15.
  */
class ScalaUDFParser extends JavaTokenParsers {
  def scalaType: Parser[PigType] = (
      "Int" ^^{ t => Types.IntType }
        | "Double" ^^{ t => Types.DoubleType }
        | "String" ^^{ t => Types.CharArrayType }
        | "Any" ^^{ t => Types.AnyType }
        | "Boolean" ^^{ t => Types.BooleanType }
        | "Long" ^^{  t => Types.LongType }
        | scalaTupleType
    )
  def scalaTupleType: Parser[PigType] = "(" ~ rep1sep(scalaType, ",") ~ ")" ^^{
    case _ ~ tlist ~ _ => val fields = tlist.zipWithIndex.map{case (t, p) => Field(s"_${p+1}", t)}.toArray
                          TupleType(fields)
  }

  def paramDecl: Parser[PigType] = ident ~ ":" ~ scalaType ^^{ case _ ~ _ ~ t => t }
  def paramList: Parser[List[PigType]] = "(" ~ repsep(paramDecl, ",") ~ ")" ^^{ case _ ~ t ~ _ => t }
  def resultTypeDecl: Parser[PigType] = scalaType ^^{ t => t }

  def udfDef: Parser[UDF] = "def" ~ ident ~ (paramList?) ~ ":" ~ resultTypeDecl ~ "=" ^^{
    case _ ~ id ~ params ~ _ ~ res ~ _  => UDF(id.toUpperCase, id, params.getOrElse(List()), res, false)
  }

  def parseDef(input: CharSequenceReader): UDF = {
    parsePhrase(input) match {
      case Success(t, _) => t
      case NoSuccess(msg, next) =>
        throw new IllegalArgumentException(s"Could not parse input string:\n${next.pos.longString} => $msg")
    }
  }

  def parsePhrase(input: CharSequenceReader): ParseResult[UDF] = phrase(udfDef)(input)
}
