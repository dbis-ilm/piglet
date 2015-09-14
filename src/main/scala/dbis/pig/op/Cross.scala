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
 * @param out the name of the output pipe (relation).
 * @param in the list of input pipes.
 */
case class Cross(out: Pipe, in: List[Pipe], timeWindow: Tuple2[Int,String]= null.asInstanceOf[Tuple2[Int,String]]) extends PigOperator {
  _outputs = List(out)
  _inputs = in

  override def lineageString: String = {
    s"""CROSS%""" + super.lineageString
  }

  override def constructSchema: Option[Schema] = {
    val newFields = ArrayBuffer[Field]()
    inputs.foreach(p => p.producer.schema match {
        case Some(s) => newFields ++= s.fields
        case None => ??? 
      })  
    schema = Some(new Schema(BagType(TupleType(newFields.toArray))))
    schema
  }

}
