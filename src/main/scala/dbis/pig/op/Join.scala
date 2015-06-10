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

import dbis.pig.schema.{TupleType, BagType, Field, Schema}

import scala.collection.mutable.ArrayBuffer

/**
 * Join represents the multiway JOIN operator of Pig.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param inPipeNames the list of names of input pipes.
 * @param fieldExprs  list of key expressions (list of keys) used as join expressions.
 */
case class Join(override val outPipeName: String, override val inPipeNames: List[String], val fieldExprs: List[List[Ref]])
  extends PigOperator(outPipeName, inPipeNames) {
  override def lineageString: String = {
    s"""JOIN%""" + super.lineageString
  }

  override def constructSchema: Option[Schema] = {
    val newFields = ArrayBuffer[Field]()
    inputs.foreach(p => p.producer.schema match {
      case Some(s) => newFields ++= s.fields
      case None => ???
    })
    schema = Some(new Schema(BagType("", TupleType("", newFields.toArray))))
    schema
  }

}

