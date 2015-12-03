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

package dbis.pig.op.cmd

import dbis.pig.op.PigOperator


/**
 * HdfsCmd represents a pseudo operator for HDFS commands.
 */
case class HdfsCmd(cmd: String, params: List[String]) extends PigOperator
{

  if (!isValid)
    throw new java.lang.IllegalArgumentException("unknown fs command '" + cmd + "'")

  _outputs = List()
  _inputs = List()

  override def outPipeNames: List[String] = List()

  def isValid: Boolean = cmd match {
    case "copyFromLocal" => true
    case "copyToRemote" => true
    case "rm" => true
    case "rmdir" => true
    case "ls" => true
    case "mkdir" => true
    case _ => false
  }

  def paramString(): String = params.map(p => s""""$p"""").mkString(",")


  override def printOperator(tab: Int): Unit = {
    println(indent(tab) + s"HDFS (cmd = ${cmd}, params = ${params.mkString(" ")})")
  }

}

