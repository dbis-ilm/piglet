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

import dbis.piglet.schema.Schema

/**
 * WindowApply is used to transform Windows back to a single continuous Stream.
 *
 * @param out the name of the output pipe.
 * @param in the name of the input pipe.
 * @param fname the name of the function which will be applied to the input window operator.
 */
case class WindowApply(
    private val out: Pipe, 
    private val in: Pipe, 
    fname: String
  ) extends PigOperator(out, in) {
  
  override def constructSchema: Option[Schema] = {
    schema
  }
  override def lineageString: String = {
    s"""WINDOWAPPLY%$fname%""" + super.lineageString
  }

  override def toString =
    s"""WINDOWAPPLY
       |  out = $outPipeName
       |  in = $inPipeName
       |  fname = $fname
     """.stripMargin

}
