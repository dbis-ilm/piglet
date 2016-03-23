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
package dbis.pig.op

import dbis.pig.schema._

import scala.collection.mutable.ArrayBuffer

/**
 * Union represents the UNION operator of Pig.
 *
 * @param initialOutPipeName the name of the output pipe (relation).
 * @param initialInPipeNames the list of names of input pipes.
 */
case class Union(out: Pipe, in: List[Pipe]) extends PigOperator(List(out), in) {

  override def lineageString: String = {
    s"""UNION%""" + super.lineageString
  }

  override def constructSchema: Option[Schema] = {
    val bagType = (p: Pipe) => p.producer.schema.get.element
    val generalizedBagType = (b1: BagType, b2: BagType) => {
      require(b1.valueType.fields.length == b2.valueType.fields.length)
      val newFields = ArrayBuffer[Field]()
      val fieldPairs = b1.valueType.fields.zip(b2.valueType.fields)
      for ((f1, f2) <- fieldPairs) {
        newFields += Field(f1.name, Types.escalateTypes(f1.fType, f2.fType))
      }
      BagType(TupleType(newFields.toArray))
    }

    // case 1: one of the input schema isn't known -> output schema = None
    if (inputs.exists(p => p.producer.schema == None)) {
      schema = None
    }
    else {
      // case 2: all input schemas have the same number of fields
      val s1 = inputs.head.producer.schema.get
      if (! inputs.tail.exists(p => s1.fields.length != p.producer.schema.get.fields.length)) {
        val typeList = inputs.map(p => bagType(p))
        val resultType = typeList.reduceLeft(generalizedBagType)
        schema = Some(Schema(resultType))
      }
      else {
        // case 3: the number of fields differ
        schema = None
      }
    }
    schema
  }

  override def printOperator(tab: Int): Unit = {
    println(indent(tab) + s"UNION { out = ${outPipeName} , in = ${inPipeNames.mkString(",")} }")
    println(indent(tab + 2) + "inSchema = " + inputSchema)
    println(indent(tab + 2) + "outSchema = " + schema)
  }
}

