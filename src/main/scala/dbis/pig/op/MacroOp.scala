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

import dbis.pig.plan.InvalidPlanException
import dbis.pig.schema.Schema
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import dbis.pig.expr.Ref
import dbis.pig.op.cmd.DefineMacroCmd
import dbis.pig.expr.NamedField

/**
 *
 * @param out the output pipe (relation).
 * @param macroName
 * @param params
 */
case class MacroOp(
    private val out: Pipe, 
    macroName: String, 
    params: Option[List[Ref]] = None
  ) extends PigOperator(out) {

  val paramMapping = Map[String, Ref]()

  private var macroDef: Option[DefineMacroCmd] = None

  def macroDefinition(): Option[DefineMacroCmd] = macroDef

  def setMacroDefinition(cmd: DefineMacroCmd): Unit = {
    /*
     * Make a deep copy of cmd
     */
    macroDef = Some(cmd.deepClone())

    /*
     * Build the mapping table for parameter values.
     */
    buildParameterMapping(cmd)
    /*
     * Adjust the input pipes: which of the params is a pipe?
     */
    params match {
      case Some(p) => {
        val pipeParams = cmd.pipeParamPositions()
        val inPipes = ListBuffer[Pipe]()
        pipeParams.foreach(i => {
          val ref = p(i)
          if (ref.isInstanceOf[NamedField]) {
            inPipes += Pipe(ref.asInstanceOf[NamedField].name)
          }
        })
        _inputs = inPipes.toList
      }
      case None => {}
    }
    /*
     * TODO: Create unique pipe names.
     */
  }

  /**
   * Constructs a table containing mappings from macro parameter names
   * to the current values of the macro call.
   *
   * @param cmd the macro DEFINE statement
   */
  def buildParameterMapping(cmd: DefineMacroCmd): Unit = {
      if (cmd.params.isEmpty && params.isDefined || cmd.params.isDefined && params.isEmpty)
        throw new InvalidPlanException(s"macro ${macroName}: parameter list doesn't match with definition")
    if (cmd.params.isDefined) {
      val defs = cmd.params.get
      val p = params.get
      if (defs.size != p.size)
        throw new InvalidPlanException(s"macro ${macroName}: number of parameters doesn't match with definition")

      for (i <- 0 to defs.size-1) {
        paramMapping += ("$" + defs(i) -> p(i))
      }
    }

    paramMapping += ("$" + cmd.out.name -> NamedField(outPipeName))
  }

  override def lineageString: String = s"""MACRO%${macroName}%""" + super.lineageString

  override def checkSchemaConformance: Boolean = {
    // TODO
    true
  }

  override def constructSchema: Option[Schema] = {
    // TODO
    super.constructSchema
  }
}

