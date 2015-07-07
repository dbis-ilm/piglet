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

case class SplitBranch(val outPipeName: String, val expr: Predicate)

/**
 * SplitInto represents the SPLIT INTO operator of Pig.
 *
 * @param initialInPipeName the names of the input pipe.
 * @param splits a list of split branches (output pipe + condition)
 */
case class SplitInto(val initialInPipeName: String, val splits: List[SplitBranch])
  extends PigOperator("", initialInPipeName) {

  override def initialOutPipeNames: List[String] = splits.map{ case branch => branch.outPipeName }

  override def lineageString: String = {
    s"""SPLIT%""" + super.lineageString
  }
}
