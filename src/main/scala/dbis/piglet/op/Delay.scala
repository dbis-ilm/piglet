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

import scala.concurrent.duration.FiniteDuration

/**
 * Delay represents the DELAY operator of Pig.
 *
 * @param out the output pipe (relation).
 * @param in the input pipe.
 * @param sampleFactor the percentage of input tuples that is passed to the output pipe
 * @param wtime the time for delaying the processing
 *
 */
case class Delay(
                  private val out: Pipe,
                  private val in: Pipe,
                  sampleFactor: Int,
                  wtime: (FiniteDuration, FiniteDuration)
  ) extends PigOperator(out, in) {

  private val r = 0 //System.currentTimeMillis()

  override def lineageString: String = {
    s"""DELAY%$sampleFactor%$wtime%$r%""" + super.lineageString
  }

  override def toString =
    s"""DELAY
        |  out = $outPipeName
        |  in = $inPipeName
        |  sample factor = $sampleFactor
        |  waiting time = ${wtime._1} - ${wtime._2}""".stripMargin



}
