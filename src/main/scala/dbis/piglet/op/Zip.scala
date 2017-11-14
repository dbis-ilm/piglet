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
package dbis.piglet.op

import dbis.piglet.schema._

import scala.collection.mutable.ArrayBuffer

/**
 * Union represents the UNION operator of Pig.
 *
 * @param out the name of the output pipe (relation).
 * @param in the list of input pipes.
 */
case class Zip(
    private val out: Pipe, 
    private val in: List[Pipe],
    withIndex: Boolean
  ) extends PigOperator(List(out), in) {

  require((in.size > 1 && !withIndex) || (in.size == 1 && withIndex), "zip with index works only with one input. Otherwise we must have at least two inputs")

  override def lineageString: String = {
    s"""ZIP%$withIndex""" + super.lineageString
  }

  override def constructSchema: Option[Schema] = {


    val newFields = inputs.flatMap(p => p.producer.schema match {
        case Some(s) =>
          s.fields.map { f =>
            Field(f.name, f.fType, p.name :: f.lineage)
          }
        case None =>
          throw new UnsupportedOperationException(s"Cannot zip with unknown Schema! (input pipe $p)")
      })



    schema = Some(Schema(
      BagType(
        TupleType(
          (if(withIndex) newFields :+ Field("index", Types.LongType) else newFields).toArray
        )
      )
    ))
    schema
  }

  override def toString =
    s"""ZIP
       |  out = ${outPipeNames.mkString(",")}
       |  in = ${inPipeNames.mkString(",")}
       |  withIndex = $withIndex""".stripMargin

}

