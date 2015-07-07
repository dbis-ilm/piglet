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
package dbis.pig.plan

import dbis.pig.op._
import dbis.pig.schema.SchemaException

import scala.collection.mutable.{ListBuffer, Map}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.mutable.Graph
import scalax.collection.GraphPredef._

/**
 * A pipe connects two Pig operator and associates a name to this channel.
 * Pipes are stored as members of the "receiver" operator and, therefore,
 * contain only the producer.
 *
 * @param name the name of the pipe
 * @param producer the operator producing the data
 */
case class Pipe (name: String, producer: PigOperator)

/**
 * An exception indicating that the dataflow plan is invalid.
 *
 * @param msg a text describing the reason
 */
case class InvalidPlanException(msg: String) extends Exception(msg)

class DataflowPlan(var operators: List[PigOperator]) {
  // private var graph = Graph[PigOperator,DiEdge]()

  /**
   * A list of JAR files specified by the REGISTER statement
   */
  val additionalJars = ListBuffer[String]()

  constructPlan(operators)

  /**
   * Constructs a plan from a list of Pig operators. This means, that based on
   * the initial input and output bag names (as specified in the script) the
   * necessary pipes are constructed and assigned to the operators.
   *
   * @param ops the list of pig operators produced by the parser
   */
  def constructPlan(ops: List[PigOperator]) : Unit = {
    def unquote(s: String): String = s.substring(1, s.length - 1)

    // This maps a String (the relations name, a string) to the single operator that writes it and the list of
    // operators that read it.
    val pipes: Map[String, (PigOperator, List[PigOperator])] = Map[String, (PigOperator, List[PigOperator])]()

    /*
     * 0. We remove all Register operators: they are just pseudo-operators.
     *    Instead, we add their arguments to the additionalJars list
     */
    ops.filter(_.isInstanceOf[Register]).foreach(op => additionalJars += unquote(op.asInstanceOf[Register].jarFile))
    val planOps = ops.filterNot(_.isInstanceOf[Register])

    /*
     * 1. We create the mapping from names to the operators that *write* them.
     */
    planOps.foreach(op => {
      // we can have multiple OutPipeName's (e.g. in SplitInto)
      op.initialOutPipeNames.foreach { name: String =>
        if (name != "") {
          if (pipes.contains(name))
            throw new InvalidPlanException("duplicate pipe: " + name)
          pipes(name) = (op, List())
        }
      }
    })

    /*
     * 2. We add operators that *read* a name to the mapping
     */
    planOps.foreach(op => {
      if (op.initialInPipeNames.nonEmpty) {
        for (inPipeName <- op.initialInPipeNames) {
          val element = pipes(inPipeName)
          pipes(inPipeName) = element.copy(_2 = element._2 :+ op)
        }
      }
    })
    /*
     * 3. We assign the pipe objects to the input and output pipes of all operators
     *    based on their names
     */
    try {
      planOps.foreach(op => {
        op.inputs = op.initialInPipeNames.map(p => Pipe(p, pipes(p)._1))
        op.output = if (op.initialOutPipeName != "") Some(op.initialOutPipeName) else None
        op.outputs = if (op.initialOutPipeName != "") pipes(op.initialOutPipeName)._2 else op.outputs
        // println("op: " + op)
        op.preparePlan
        op.constructSchema
      })
    }
    catch {
      case e: java.util.NoSuchElementException => throw new InvalidPlanException("invalid pipe: " + e.getMessage)
    }
    operators = planOps
  }

  /**
   * Returns a set of operators acting as sinks (without output bags) in the dataflow plan.
   *
   * @return the set of sink operators
   */
  def sinkNodes: Set[PigOperator] = operators.filter((n: PigOperator) => n.outputs.isEmpty).toSet[PigOperator]

  /**
   * Returns a set of operators acting as sources (without input bags) in the dataflow plan.
   *
   * @return the set of source operators
   */
  def sourceNodes: Set[PigOperator] = operators.filter((n: PigOperator) => n.inputs.isEmpty).toSet[PigOperator]

  /**
   * Checks whether the dataflow plan represents a connected graph, i.e. all operators have their
   * input.
   *
   * @return true if the plan is connected, false otherwise
   */
  def checkConnectivity: Boolean = {
    // TODO: check connectivity of subplans in nested foreach
    // we simply construct a graph and check its connectivity
    var graph = Graph[PigOperator,DiEdge]()
    operators.foreach(op => op.inputs.foreach(p => graph += p.producer ~> op))
    graph.isConnected
  }

  /**
   * Checks whether all operators and their expressions conform to the schema
   * of their input bags. If not, a SchemaException is raised.
   */
  def checkSchemaConformance: Unit = {
    val errors = operators.view.map{ op => (op, op.checkSchemaConformance) }
                    .filter{ t => t._2 == false }

    if(!errors.isEmpty) {
      val str = errors.map(_._1).mkString(" and ")
      throw SchemaException(str)
    }
    
//    operators.map(_.checkSchemaConformance).foldLeft(true){ (b1: Boolean, b2: Boolean) => b1 && b2 }
  }

  /**
   * Returns the operator that produces the relation with the given alias.
   *
   * @param s the alias name of the output relation
   * @return the operator producing this relation
   */
  def findOperatorForAlias(s: String): Option[PigOperator] = operators.find(o => o.outPipeName == s)

  /**
   * Returns a list of operators satisfying a given predicate.
   *
   * @param pred a function implementing a predicate
   * @return the list of operators satisfying this predicate
   */
  def findOperator(pred: PigOperator => Boolean) : List[PigOperator] = operators.filter(n => pred(n))

  /**
   * Swaps the two operators in the dataflow plan. Both operators are unary operators and have to be already
   * part of the plan.
   *
   * @param n1 the first operator
   * @param n2 the second operator
   * @return the resulting dataflow plan
   */
  def swap(n1: PigOperator, n2: PigOperator) : DataflowPlan = {
    this
  }

  /**
   * Inserts the operator op after the given operator old in the dataflow plan. old has to be already part of the plan.
   *
   * @param old the operator after we insert
   * @param op the new operator to be inserted after old
   * @return the resulting dataflow plan
   */
  def insertAfter(old: PigOperator, op: PigOperator) : DataflowPlan =  {
    this
  }

  /**
   * Remove the given operator from the dataflow plan.
   *
   * @param n the operator to be removed from the plan
   * @return the resulting dataflow plan
   */
  def remove(n: PigOperator) : DataflowPlan = {
    this
  }

  /**
   * Replace the operator old by the new operator repl in the current dataflow plan.
   *
   * @param old the operator which has to be replaced
   * @param repl the new operator
   * @return the resulting dataflow plan
   */
  def replace(old: PigOperator, repl: PigOperator) : DataflowPlan =  {
    this
  }

}
