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

import java.security.MessageDigest

import dbis.pig.plan.Pipe
import dbis.pig.schema._
import org.kiama.rewriting.Rewritable
import scala.collection.immutable.Seq

/**
 * PigOperator is the base class for all Pig operators. An operator contains
 * pipes representing the input and output connections to other operators in the
 * dataflow.
 *
 * @param initialOutPipeName the name of the initial output pipe (relation) which is needed to construct the plan, but
 *                           can be changed later.
 * @param initialInPipeNames the list of names of initial input pipes.
 * @param schema
 */
abstract class PigOperator (val initialOutPipeName: String, val initialInPipeNames: List[String], var schema:
Option[Schema]) extends Rewritable{
  // A list of all pipes that this operator reads from.
  var inputs: List[Pipe] = List[Pipe]()

  // If the operator writes a relation, this is its name.
  private var _output: Option[String] = None
  def output = _output

  @throws[IllegalStateException]("if an output name is going to be unset, but there are outputs")
  @throws[IllegalArgumentException]("if any of the existing outputs doesn't read from the new name")
  def output_=(newOutput: Option[String]) = _outputs match {
    case Nil => _output = newOutput
    case list => {
      newOutput match {
        case Some(name) => {
          list.foreach(op => {
            if (!op.initialInPipeNames.contains(name)) {
              throw new IllegalArgumentException(op + " does not read from " + newOutput)
            }
          })
          _output = newOutput
        }
        case None => {
          throw new IllegalStateException("Can't unset the relation name of an operator with outputs")
        }
      }
    }
  }


  // If the operator writes a relation, this is a list of all operators the read that one.
  private var _outputs: List[PigOperator] = List[PigOperator]()
  def outputs = _outputs

  @throws[IllegalStateException]("if the operator doesn't return a relation")
  @throws[IllegalArgumentException]("if newOps is not empty and any of the new operators doesn't read from this " +
    "operator")
  def outputs_=(newOps: List[PigOperator]) = newOps match {
    case Nil => _outputs = newOps
    case _ => {
      if (output.isEmpty) {
        throw new IllegalStateException("Can't set the outputs of an operator that doesn't return a relation")
      }
      newOps.foreach(op => {
        if (!op.initialInPipeNames.contains(output.get)) {
          throw new IllegalArgumentException(op + " does not read from " + output.get)
        }
      })
      _outputs = newOps
    }
  }

  def this(out: String, in: List[String]) = this(out, in, None)

  def this(out: String) = this(out, List(), None)

  def this(out: String, in: String) = this(out, List(in), None)

  def outPipeName: String = _output match {
    case Some(name) => name
    case None => ""
  }

  def inPipeName: String = inPipeNames.head
  def inPipeNames: List[String] = if (inputs.isEmpty) initialInPipeNames else inputs.map(p => p.name)

  def inputSchema =   if (inputs.nonEmpty) inputs.head.producer.schema else None

  def preparePlan: Unit = {}

  /**
   * Constructs the output schema of this operator based on the input + the semantics of the operator.
   * The default implementation is to simply take over the schema of the input operator.
   *
   * @return the output schema
   */
  def constructSchema: Option[Schema] = {
    if (inputs.nonEmpty)
      schema = inputs.head.producer.schema
    schema
  }

  /**
   * Returns a string representation of the output schema of the operator.
   *
   * @return a string describing the schema
   */
  def schemaToString: String = {
    /*
     * schemaToString is mainly called from DESCRIBE. Thus, we can take outPipeName as relation name.
     */
    schema match {
      case Some(s) => s"${outPipeName}: ${s.element.descriptionString}"
      case None => s"Schema for ${outPipeName} unknown."
    }
  }

  /**
   * A helper function for traversing expression trees:
   *
   * Checks the (named) fields referenced in the expression (if any) if they conform to
   * the schema. Should be overridden in operators changing the schema by invoking
   * traverse with one of the traverser function.
   *
   * @return true if valid field references, otherwise false
   */
  def checkSchemaConformance: Boolean = true

  /**
   * Returns a MD5 hash string representing the sub-plan producing the input for this operator.
   *
   * @return the MD5 hash string
   */
  def lineageSignature: String = {
    val digest = MessageDigest.getInstance("MD5")
    digest.digest(lineageString.getBytes).map("%02x".format(_)).mkString
  }

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  def lineageString: String = {
    inputs.map(p => p.producer.lineageString).mkString("%")
  }
  def arity = this.inputs.length

  def deconstruct = this.inputs

  def reconstruct(output: Seq[Any]): PigOperator = output match {
    case inputs: Seq[_] => {
      this match {
        case obj: PigOperator => {
          obj.inputs = inputs.toList.asInstanceOf[List[Pipe]]
          obj
        }
      }
    }
    case _ => illegalArgs("PigOperator", "Pipe", output)
  }
}
