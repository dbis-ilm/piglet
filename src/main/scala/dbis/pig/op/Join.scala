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
import java.util.concurrent.TimeUnit
import dbis.pig.expr.Ref


/**
 * Join represents the multiway JOIN operator of Pig.
 *
 * @param out the output pipe (relation).
 * @param in the list of input pipes.
 * @param fieldExprs  list of key expressions (list of keys) used as join expressions.
 * @param timeWindow window definition for streaming joins
 */
case class Join(
    private val out:Pipe, 
    private val in: List[Pipe], 
    fieldExprs: List[List[Ref]],
    timeWindow: Tuple2[Int,String] = null.asInstanceOf[Tuple2[Int,String]]
  ) extends PigOperator(List(out), in) {

  override def lineageString: String = {
    s"""JOIN%${fieldExprs}%""" + super.lineageString
  }

  override def constructSchema: Option[Schema] = {
    val newFields = ArrayBuffer[Field]()
    inputs.foreach(p => p.producer.schema match {
      case Some(s) => newFields ++= s.fields map { f =>
        Field(f.name, f.fType, p.name :: f.lineage)
      }
      case None => newFields += Field("", Types.ByteArrayType)
    })
    schema = Some(Schema(BagType(TupleType(newFields.toArray))))
    schema
  }

  override def printOperator(tab: Int): Unit = {
    println(indent(tab) + s"JOIN { out = ${outPipeNames.mkString(",")} , in = ${inPipeNames.mkString(",")} }")
    println(indent(tab + 2) + "inSchema = {}")
    println(indent(tab + 2) + "outSchema = " + schema)
  }

}

