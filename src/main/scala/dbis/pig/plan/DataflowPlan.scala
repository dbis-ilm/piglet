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
import dbis.pig.plan.rewriting.Rewriter
import dbis.pig.schema.SchemaException

import scala.collection.mutable.{ListBuffer, Map}

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

    // This maps a String (the relation name, a string) to the pipe that writes it and the list of
    // operators that read it.
    val pipes: Map[String, Pipe] = Map[String, Pipe]()

    /*
     * 0. We remove all Register operators: they are just pseudo-operators.
     *    Instead, we add their arguments to the additionalJars list
     */
    ops.filter(_.isInstanceOf[Register]).foreach(op => additionalJars += unquote(op.asInstanceOf[Register].jarFile))
    val planOps = ops.filterNot(_.isInstanceOf[Register])

    /*
     * 1. We create a Map from names to the pipes that *write* them.
     */
    planOps.foreach(op => {
      // we can have multiple outputs (e.g. in SplitInto)
      op.outputs.foreach { p: Pipe =>
        if (p.name != "") {
          if (pipes.contains(p.name)) {
            throw new InvalidPlanException("duplicate pipe: " + p.name)
          }
          // we initialize the producer of the pipe
          p.producer = op
          pipes(p.name) = p
        }
      }
    })

    /*
     * 2. We add operators that *read* from a pipe to this pipe
     */
    // TODO: replace by PigOperator.addConsumer
    planOps.foreach(op => {
        for (p <- op.inputs) {
          val element = pipes(p.name)
          // Pipes already have their consumers set up after rewriting, therefore this step is not necessary. In
          // fact, it would create duplicate elements in `consumer`.
          if (!(element.consumer contains op)) {
            element.consumer = element.consumer :+ op
          }
        }
    })

    /*
     * 3. Because we have completed only the pipes from the operator outputs
     *    we have to replace the inputs list of each operator
     */
    try {
      planOps.foreach(op => {
        val newPipes = op.inputs.map(p => pipes(p.name))
        op.inputs = newPipes
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
    // we simply construct a graph and check its connectivity
    /*
    var graph = Graph[PigOperator,DiEdge]()
    operators.foreach(op => op.inputs.foreach(p => graph += p.producer ~> op))
    graph.isConnected
    */
    /*
     * make sure that for all operators the following holds:
     * (1) all input pipes have a producer
     * (2) all output pipes have a non-empty consumer list
     */
    // println("DataflowPlan.checkConnectivity")
    var result: Boolean = true
    operators.foreach { op => {
      // println("check operator: " + op)
      if (!op.inputs.forall(p => p.producer != null)) {
        println("op: " + op + " : invalid input pipes")
        result = false
      }
      if (!op.outputs.forall(p => p.consumer.nonEmpty)) {
        println("op: " + op + " : invalid output pipes")
        result = false
      }
      if (!op.checkConnectivity) {
        println("op: " + op + " : not connected")
        result = false
      }
    }
    }
    result
    /*
    operators.forall(op =>
      op.inputs.forall(p => p.producer != null) && op.outputs.forall(p => p.consumer.nonEmpty) && op.checkConnectivity)
      */
  }

  def checkConsistency: Boolean = operators.forall(_.checkConsistency)

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

  def containsOperator(op: PigOperator): Boolean = operators.contains(op)
  
  /**
   * Swaps two successive operators in the dataflow plan. Both operators are unary operators and have to be already
   * part of the plan.
   *
   * @param n1 the parent operator
   * @param n2 the child operator
   * @return the resulting dataflow plan
   */
  def swap(n1: PigOperator, n2: PigOperator) : DataflowPlan = {
    Rewriter.swap(this, n1, n2)
  }

  /**
   * Inserts the operator op after the given operator old in the dataflow plan. old has to be already part of the plan.
   *
   * @param old the operator after we insert
   * @param op the new operator to be inserted after old
   * @return the resulting dataflow plan
   */
  def insertAfter(old: PigOperator, op: PigOperator) : DataflowPlan =  {
    
    val p = new Pipe(old.outPipeName, old, List(op))
    
    old.outputs = (old.outputs :+ p)
    
    op.inputs = (op.inputs :+ p)
    
    operators = operators :+ op
    
    this
  }

  /**
   * Remove the given operator from the dataflow plan.
   *
   * @param op the operator to be removed from the plan
   * @return the resulting dataflow plan
   */
  def remove(op: PigOperator) : DataflowPlan = {
    require(operators.contains(op), "operator to remove is not member of the plan")
    Rewriter.remove(this, op)
  }

  /**
   * Replace the operator old by the new operator repl in the current dataflow plan.
   *
   * @param old the operator which has to be replaced
   * @param repl the new operator
   * @return the resulting dataflow plan
   */
  def replace(old: PigOperator, repl: PigOperator) : DataflowPlan = {
    require(operators.contains(old), "operator to replace is not member of the plan")
    Rewriter.replace(this, old, repl)
  }

  def disconnect(op1: PigOperator, op2: PigOperator): DataflowPlan = {
    require(operators.contains(op1), s"operator is not member of the plan: $op1")
    require(operators.contains(op2), s"operator is not member of the plan: $op2")
    
    op2.inputs = op2.inputs.filter { op => op.producer != op1 }
    op1.outputs = op1.outputs.filter { op => op != op2 }
    
    
    this
  }
}
