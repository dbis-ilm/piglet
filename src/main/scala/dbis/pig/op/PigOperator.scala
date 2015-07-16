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
  protected var _outputs: List[Pipe] = _
  protected var _inputs: List[Pipe]  = _

  var schema: Option[Schema] = None

  /**
   * Getter method for the output pipes.
   *
   * @return the list of output pipes
   */
  def outputs = _outputs

  /**
   * Setter method for the output pipes. It ensures
   * that this is producer of all pipes.
   *
   * @param o the new list of output pipes
   */
  def outputs_=(o: List[Pipe]) = {
    _outputs = o
    // make sure that we are producer in all pipes
    _outputs.foreach(p => p.producer = this)
  }

  /**
   * Getter method for the input pipes.
   *
   * @return the list of input pipes
   */
  def inputs = _inputs

  /**
   * Setter method for the input pipes. It ensures
   * that this is a consumer in all pipes.
   *
   * @param i the new list of input pipes
   */
  def inputs_=(i: List[Pipe]) = {
    _inputs = i
    // make sure that we are consumer in all pipes
    _inputs.foreach(p => if (!p.consumer.contains(this)) p.consumer = p.consumer :+ this)
  }

  def outPipeName: String = if (outputs.nonEmpty) outputs.head.name else ""

  def inputSchema = if (inputs.nonEmpty) inputs.head.inputSchema else None

  def preparePlan: Unit = {}

  def checkConnectivity: Boolean = true

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
        case None =>
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
      case Some(s) => s"$outPipeName: ${s.element.descriptionString}"
      case None => s"Schema for $outPipeName unknown."
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

  /**
   * Check whether the input and output pipes are still consistent, i.e.
   * for all output pipes the producer is the current operator and the current
   * operator is also a consumer in each input pipe.
   *
   * @return true if the operator pipes are consistent
   */
  def checkConsistency: Boolean = {
    outputs.forall(p => p.producer == this) && inputs.forall(p => p.consumer.contains(this))
  }

  /**
   * Returns the arity, i.e. the number of output pipes of
   * this operator.
   *
   * @return the arity of the operator
   */
  def arity = {
    var numConsumers = 0
    this.outputs.foreach(p => numConsumers += p.consumer.length)
    numConsumers
  }

  def deconstruct = this.outputs.map(_.consumer)

  def reconstruct(outputs: Seq[Any]): PigOperator = {
    val outname = this.outPipeName
    reconstruct(outputs, outname)
  }

  /** Implementation for kiamas Rewritable trait
    *
    * It's necessary to set the `outputs` attribute on this object to List.empty, which makes `this.outPipeName`
    * return "". To work around this, the output name can be provided via `outname`.
    *
    * @param outputs
    * @param outname The output name of this relation
    * @return
    */
  def reconstruct(outputs: Seq[Any], outname: String): PigOperator = {
    this.outputs = List.empty
    outputs.foreach(_ match {
      case op : PigOperator => {
        val idx = this.outputs.indexWhere(_.name == outname)
        if (idx > -1) {
          // There already is a Pipe to `outname`
          this.outputs(idx).consumer = this.outputs(idx).consumer :+ op
        } else {
                  this.outputs = this.outputs :+ Pipe(outname, this, List(op))
        }
      }
      // Some rewriting rules turn one operator into multiple ones, for example Split Into into multiple Filter
      // operators
      case ops: List[_] => this.reconstruct(ops, outname)
      case _ => illegalArgs("PigOperator", "PigOperator", outputs)
    })
    this
  }
}
