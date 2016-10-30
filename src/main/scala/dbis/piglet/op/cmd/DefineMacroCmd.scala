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

import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}
import dbis.piglet.plan.DataflowPlan
import scala.collection.mutable.ListBuffer
import dbis.piglet.op.{Pipe,PigOperator}


case class DefineMacroCmd(
    out: Pipe, 
    macroName: String, 
    params: Option[List[String]], 
    stmts: List[PigOperator]
  ) extends PigOperator(out) {

  var subPlan: Option[DataflowPlan] = None
  var inPipes = List[Pipe]()

  def deepClone(): DefineMacroCmd = {
      val baos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(baos)
      oos.writeObject(this)
      val bais = new ByteArrayInputStream(baos.toByteArray())
      val ois = new ObjectInputStream(bais)
      ois.readObject().asInstanceOf[DefineMacroCmd]
  }

  override def preparePlan: Unit = {
    /*
     * First, we try to identify pipes from the input parameter list.
     */
    val inputs = params match {
      case Some(p) => {
        /*
         * We collect potential input pipes.
         */
        val pipes = collectPipes(stmts)

        /*
         * Next, we check which parameter refers to a pipe.
         */
        val pp = p.map(s => "$" + s)
        inPipes = pipes.filter(pi => pp.contains(pi.name))
        inPipes
      }
      case None => List()
    }
    /*
     * We construct a subplan for the operator list
     * and add our input pipe(s) to the context of the plan.
     */
    subPlan = Some(new DataflowPlan(stmts, Some(inputs)))
  }

  /**
   * Collect all input pipes of the operators in the given list.
   *
   * @param ops a list of Pig operators
   * @return the list of all input pipes
   */
  def collectPipes(ops: List[PigOperator]): List[Pipe] = {
    val pipes = ListBuffer[Pipe]()
    ops.foreach{op => op.inputs.foreach(p => pipes += p)}
    pipes.toList
  }

  /**
   * Construct a list of positions in the macro parameter list corresponding
   * to pipe parameters.
   *
   * @return
   */
  def pipeParamPositions(): List[Int] = {
    val l = ListBuffer[Int]()
    inPipes.foreach(i => {
      val pos = params.get.indexOf(i.name.substring(1))
      if (pos >= 0) l += pos
    })
    l.toList
  }
}
