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

/**
  * Window represents the WINDOW operator of Pig.
  *
  * @param out the name of the output pipe
  * @param in the name of the input pipe
  * @param window the window size and time unit 
  * @param slide the windows slider frequency (every) and time unit
  * @param applyData informations(func name, output schema, func body) needed for window apply function
  */
case class Window(
    private val out:Pipe, 
    private val in: Pipe, 
    window: Tuple2[Int,String], 
    slide: Tuple2[Int,String]
  ) extends PigOperator(out, in) {

  /** 
    * Returns the lineage string describing the sub-plan producing the input for this operator.
    *
    * @return a string representation of the sub-plan.
    */
  override def lineageString: String = { 
    s"""WINDOW%${window}%""" + super.lineageString
  }
}
