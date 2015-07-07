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

import dbis.pig.schema._
import org.kiama.rewriting.Rewritable
import scala.collection.immutable.Seq

/**
 * PigOperator is the base trait for all Pig operators. An operator contains
 * pipes representing the input and output connections to other operators in the
 * dataflow.
 */
trait PigOperator extends Rewritable {
  var outputs: List[Pipe] = _
  var inputs: List[Pipe]  = _
  var schema: Option[Schema] = None

  def outPipeName: String = if (outputs.nonEmpty) outputs.head.name else ""

  def inputSchema = if (inputs.nonEmpty ) inputs.head.inputSchema else None

  def preparePlan: Unit = {}

  /**
   * Constructs the output schema of this operator based on the input + the semantics of the operator.
   * The default implementation is to simply take over the schema of the input operator.
   *
   * @return the output schema
   */
  def constructSchema: Option[Schema] = {
    if (inputs.nonEmpty) {
      schema = inputs.head.producer.schema
      // the bag should be named with the output pipe
      schema match {
        case Some(s) => s.setBagName(outPipeName)
        case None => {}
      }
    }
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
