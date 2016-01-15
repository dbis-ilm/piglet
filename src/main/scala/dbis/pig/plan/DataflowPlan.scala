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
import dbis.pig.op.cmd._
import dbis.pig.expr._

import dbis.pig.plan.rewriting.Rewriter
import dbis.pig.schema.{Types, PigType, Schema, SchemaException}
import dbis.pig.udf.{UDFTable, UDF}
import scala.collection.mutable.{ListBuffer, Map}



/**
 * An exception indicating that the dataflow plan is invalid.
 *
 * @param msg a text describing the reason
 */
case class InvalidPlanException(msg: String) extends Exception(msg)

/**
 * A DataflowPlan is a graph of operators representing a Piglet script and provides methods
 * to construct the graph from a list of PigOperators with their pipes as well as to check and manipulate
 * the graph.
 *
 * @param _operators a list of operators used to construct the plan
 * @param ctx an optional list of pipes representing the context, i.e. the
 *            pipes of a nesting operator (e.g. FOREACH).
 */
class DataflowPlan(private var _operators: List[PigOperator], val ctx: Option[List[Pipe]] = None) extends Serializable {
  def operators_=(ops: List[PigOperator]) = {
    ops map { _.outPipeNames map { PipeNameGenerator.addKnownName}}
    _operators = ops
  }

  def operators = _operators

  /**
   * A list of JAR files specified by the REGISTER statement
   */
  val additionalJars = ListBuffer[String]()

  /**
   * A map for UDF aliases + constructor arguments.
   */
  val udfAliases = Map[String,(String, List[Any])]()

  var code: String = ""
  var extraRuleCode: Seq[String] = List.empty

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
    var pipes = Map[String, Pipe]()

    // This maps macro names to their definitions
    val macros = Map[String, DefineMacroCmd]()

    /*
     * 1. We remove all REGISTER, DEFINE, SET, and embedded code operators: they are just pseudo-operators.
     *    Instead, for REGISTER and DEFINE we add their arguments to the additionalJars list and udfAliases map
     */
    ops.filter(_.isInstanceOf[RegisterCmd]).foreach(op => additionalJars += op.asInstanceOf[RegisterCmd].jarFile)
    ops.filter(_.isInstanceOf[DefineCmd]).foreach { op =>
      val defineOp = op.asInstanceOf[DefineCmd]
      udfAliases += (defineOp.alias ->(defineOp.scalaName, defineOp.paramList))
    }
    ops.filter(_.isInstanceOf[EmbedCmd]).foreach(op => {
      val castedOp = op.asInstanceOf[EmbedCmd]
      code += castedOp.code
      castedOp.extractUDFs
      castedOp.ruleCode.foreach { c: String => extraRuleCode = extraRuleCode :+ c}
    })

    /*
     * 2. We collect all macro definitions
     */
    ops.filter(_.isInstanceOf[DefineMacroCmd]).foreach(op => {
      val macroOp = op.asInstanceOf[DefineMacroCmd]
      macroOp.preparePlan
      macros += (macroOp.macroName -> macroOp)
    })

    /*
     * 3. We assign to each MacroOp the corresponding definition. Note, that the actual replacement
     * is done later in the rewriting phase
     */
    ops.filter(_.isInstanceOf[MacroOp]).foreach(op => {
      val macroOp = op.asInstanceOf[MacroOp]
      if (macros.contains(macroOp.macroName))
        macroOp.setMacroDefinition(macros(macroOp.macroName))
    })

    /*
     * 4. ... and finally, filter out all commands not representing a dataflow operator
     */
    val allOps = ops.filterNot(o =>
      o.isInstanceOf[RegisterCmd] ||
      o.isInstanceOf[DefineCmd] ||
      o.isInstanceOf[EmbedCmd] ||
      o.isInstanceOf[DefineMacroCmd]
    )
    val planOps = processSetCmds(allOps).filterNot(_.isInstanceOf[SetCmd])

    /*
     * 5. We create a Map from names to the pipes that *write* them.
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
     * 6. We add operators that *read* from a pipe to this pipe
     */
    // TODO: replace by PigOperator.addConsumer
    var varCnt = 1
    val newOps = ListBuffer[(PigOperator,PigOperator)]()
    planOps.foreach(op => {
        for (p <- op.inputs) {
          if (! pipes.contains(p.name)) {
            /*
             * If we cannot find the pipe we look in the context: perhaps we can get it from the outer
             * scope of a nested statement
             */
            val outerPipe = resolvePipeFromContext(p.name)
            outerPipe match {
              case Some(oPipe) => {
                if (oPipe.name != p.name) {
                  /*
                   * The outer pipe name is different from the input pipe. Thus, we have to insert
                   * a ConstructBag operator before the current operator. For this purpose, we collect
                   * pairs of (ConstructBag, operator) which we later insert.
                   */
                  val cbOp = ConstructBag(Pipe(s"${p.name}_${varCnt}"), DerefTuple(NamedField(oPipe.name), NamedField(p.name)))
                  newOps += ((cbOp, op))
                  varCnt += 1
                }
              }
              case None => throw new InvalidPlanException("invalid pipe: " + p.name + " for " + op)
            }
          }
          else {
            val element = pipes(p.name)
            // Pipes already have their consumers set up after rewriting, therefore this step is not necessary. In
            // fact, it would create duplicate elements in `consumer`.
            if (!(element.consumer contains op)) {
              element.consumer = element.consumer :+ op
            }
          }
        }
    })

    /*
     * 7. If new operators were constructed we have to insert them now.
     */
    val (newOpList, newPipes) = insertOperators(planOps, newOps.toList, pipes)
    pipes = newPipes

    /*
     * 8. Because we have completed only the pipes from the operator outputs
     *    we have to replace the inputs list of each operator
     */
    try {
      newOpList.foreach(op => {
        // we ignore $name here because this appears only inside a macro
        if (op.inputs.filter(p => p.name.startsWith("$")).isEmpty) {
          val newPipes = op.inputs.map(p => pipes(p.name))
          op.inputs = newPipes
        }
        op.preparePlan
        op.constructSchema
      })
    }
    catch {
      case e: java.util.NoSuchElementException => throw new InvalidPlanException("invalid pipe: " + e.getMessage)
    }
    operators = newOpList.toList
  }

  /**
   * Insert an operator from the newOps list into the list opList. The position of the new operator
   * is specified by the newOps pair (newOp, currOp), i.e. newOp is inserted before currOp and the pipes
   * are updated accordingly.
   *
   * @param opList the current list of operators
   * @param newOps a list of pairs (newOp, currOp) determining the operators to be inserted as well as
   *               their position
   * @param pipes the map of pipes
   * @return the updated operator list + the updated pipe map
   */
  def insertOperators(opList: List[PigOperator], newOps: List[(PigOperator, PigOperator)],
                      pipes: Map[String, Pipe]): (List[PigOperator], Map[String, Pipe]) = {

    /*
     * Replace the name of the pipe with the oName by the name of the pipe.
     * Note, that it isn't necessary to fix all references here, because we
     * do this in constructPlan anyway.
     */
    def replacePipe(op: PigOperator, oName: String, pipe: Pipe): Unit = {
      op.inputs.foreach(p => if (p.name == oName) { p.name = pipe.name})
    }

    if (newOps.nonEmpty) {
      val newOpList = opList.to[ListBuffer]
      newOps.foreach(pair => {
        val op = pair._1
        val consumer = pair._2
        /*
         * Update the pipe
         */
        val pipe = op.outputs.head
        pipe.addConsumer(consumer)
        pipe.producer = op
        pipes(pipe.name) = pipe
        /*
         * Replace the input pipe
         */
        val oldPipeName = pipe.name.substring(0, pipe.name.lastIndexOf("_"))
        replacePipe(consumer, oldPipeName, pipe)
        /*
         * Insert the new operator before the consumer
         */
        val pos = newOpList.indexOf(consumer)
        newOpList.insert(pos, op)
      })
      (newOpList.toList, pipes)
    }
    else
      (opList, pipes)
  }

  /**
   * Try to find the pipe in the context, i.e. as an element of an relation of the outer
   * operator. This is used only for nested statements.
   *
   * @param pName the name of the pipe we are trying to find
   * @return the pipe if it exists
   */
  def resolvePipeFromContext(pName: String): Option[Pipe] = {
    if (ctx.isEmpty) None
    else {
      var res: Option[Pipe] = None
      for (pipe <- ctx.get) {
        pipe.inputSchema match {
          case Some(schema) => if (schema.indexOfField(pName) != -1) res = Some(pipe)
          case None => res = Some(pipe) // None
        }
      }
      res
    }
  }

  /**
   * Looks for SetCmd operators in the operator list and apply the parameter to
   * each subsequent operator in the list.
   */
  def processSetCmds(opList: List[PigOperator]): List[PigOperator] = {
    val currentParams: Map[String, Any] = Map()
    opList.foreach(op => op match {
      case SetCmd(key, value) => currentParams += (key -> value)
      case _ => op.configParams = op.configParams ++ currentParams
      }
    )
    opList
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

  /**
   * Checks whether the plan contains the given operator.
   *
   * @param op the operator we are looking for
   * @return true if the operator exists
   */
  def containsOperator(op: PigOperator): Boolean = operators.contains(op)

  /**
   * Prints a textual representation of the plan to standard output.
   *
   * @param tab the number of whitespaces for indention
   */
  def printPlan(tab: Int = 0): Unit = operators.foreach(_.printOperator(tab))

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
    Rewriter.insertAfter(this, old, op)
  }
 
  /**
   * Inserts the operator op between the given operators inOp and outOp in the dataflow plan. inOp and oldOp has to be already part of the plan.
   *
   * @param inOp the operator after we insert
   * @param outOp the operator before we insert
   * @param op the new operator to be inserted in between
   * @return the resulting dataflow plan
   */
  def insertBetween(inOp: PigOperator, outOp: PigOperator, op: PigOperator) : DataflowPlan =  {
    Rewriter.insertBetween(this, inOp, outOp, op)
  }


  /**
   * Remove the given operator from the dataflow plan.
   *
   * @param op the operator to be removed from the plan
   * @param removePredecessors If true, predecessors of `rem` will be removed as well.
   * @return the resulting dataflow plan
   */
  def remove(op: PigOperator, removePredecessors: Boolean = false) : DataflowPlan = {
    require(operators.contains(op), "operator to remove is not member of the plan")
    Rewriter.remove(this, op, removePredecessors)
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

  def resolveParameters(mapping: Map[String, Ref]): Unit = operators.foreach(p => p.resolveParameters(mapping))

  /**
    * Check all statements using the given expression traverser if any of the expressions
    * satisfies the predicate.
    *
    * @param f an expression traverser
    * @return true if any expression was found satisfying the predicate of the traverser
    */
  def checkExpressions(f: (Schema, Expr) => Boolean): Boolean = {
    val res = operators.map(_ match {
      case op@Foreach(_, _, gen, _) => gen match {
        case GeneratorList(exprs) => exprs.exists(g => f(op.schema.orNull, g.expr))
        case GeneratorPlan(p) => false
      }
      case op@Filter(_, _, pred, _) => pred.traverseOr(op.schema.orNull, f)
      case _ => false
    })
    res.contains(true)
  }
}
