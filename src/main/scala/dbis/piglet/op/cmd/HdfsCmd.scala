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

package dbis.piglet.op.cmd

import dbis.piglet.op.PigOperator
import dbis.piglet.tools.HDFSService
import dbis.piglet.tools.HdfsCommand


/**
 * HdfsCmd represents a pseudo operator for HDFS commands.
 */
case class HdfsCmd(cmd: HdfsCommand.HdfsCommand, params: List[String]) extends PigOperator(List(), List())
{

//  if (!isValid)
//    throw new java.lang.IllegalArgumentException("unknown fs command '" + cmd + "'")


  override def outPipeNames: List[String] = List()

//  def isValid = HdfsCommand.values.map{v => v.toString().toLowerCase()}.exists { s => s.equalsIgnoreCase(cmd) }

  def paramString(): String = params.map(p => s""""$p"""").mkString(",")

  override def toString =
    s"""HDFS COMMAND
       |  cmd = $cmd
       |  params = ${params.mkString(",")}""".stripMargin

}

