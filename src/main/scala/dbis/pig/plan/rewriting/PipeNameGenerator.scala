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
package dbis.pig.plan.rewriting

import scala.util.Random

/** Implements generating random pipe names.
  *
  */
object PipeNameGenerator {
  /** The length of the generated pipe names.
    *
    */
  final val length = 10

  /** Characters allowed in the generated pipe names.
    *
    */
  final val characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

  /** The number of characters in [[characters]].
    *
    */
  final lazy val numChars = characters.length

  /** The prefix of the pipe name.
    *
    */
  final val prefix = "pipe"

  /** Generate a pipe name of the specified `length`.
    *
    * @param length
    * @return
    */
  def generate(length: Int): String = prefix ++ recGenerate(length)

  private def recGenerate(length: Int): String = {
    length match {
      case 0 => ""
      case n => characters(Random.nextInt(numChars)).toString ++ recGenerate(n - 1)
    }
  }

  /** Generate a pipe name of length [[length]].
    *
    * @return
    */
  def generate(): String = generate(length)
}
