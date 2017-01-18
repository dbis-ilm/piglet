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
package dbis.piglet.op

import dbis.piglet.plan.{InvalidPlanException, DataflowPlan}
import dbis.piglet.schema._
import scala.collection.mutable.{ListBuffer, Map}
import dbis.piglet.expr.ArithmeticExpr
import dbis.piglet.expr.Ref
import dbis.piglet.expr.Expr


/**
 * A trait for the GENERATE part of a FOREACH operator.
 */
trait ForeachGenerator {
  def isNested: Boolean
}

/**
 * GeneratorExpr represents a single expression of a Generator.
 *
 * @param expr
 * @param alias
 */
case class GeneratorExpr(expr: ArithmeticExpr, alias: Option[Field] = None) {
  override def toString = expr + (if (alias.isDefined) s" -> ${alias.get}" else "")
}

/**
 * GeneratorList implements the ForeachGenerator trait and is used to represent
 * the FOREACH ... GENERATE operator.
 *
 * @param exprs
 */
case class GeneratorList(var exprs: List[GeneratorExpr]) extends ForeachGenerator {
  def constructFieldList(inputSchema: Option[Schema]): Array[Field] =
    exprs.map(e => {
      e.alias match {
        // if we have an explicit schema (i.e. a field) then we use it
        case Some(f) => {
          if (f.fType == Types.ByteArrayType) {
            // if the type was only bytearray, we should check the expression if we have a more
            // specific type
            val res = e.expr.resultType(inputSchema)
            Field(f.name, res)
          }
          else
            f
        }
        // otherwise we take the field name from the expression and
        // the input schema
        case None => {
          val res = e.expr.resultType(inputSchema)
          Field(e.expr.exprName, res)
        }
      }
    }).toArray

  def isNested() = false
}

/**
 * GeneratorPlan implements the ForeachGenerator trait and is used to represent
 * a nested FOREACH.
 *
 * @param subPlan
 */
case class GeneratorPlan(var subPlan: List[PigOperator]) extends ForeachGenerator {
    def isNested() = true
}

/**
 * Foreach represents the FOREACH operator of Pig.
 *
 * @param out the output pipe (relation).
 * @param in the input pipe
 * @param generator the generator (a list of expressions or a subplan)
 * @param windowMode ???
 */
case class Foreach(
    private val out: Pipe,
    private val in: Pipe,
    var generator: ForeachGenerator,
    var windowMode: Boolean = false
  ) extends PigOperator(out, in) {

  var subPlan: Option[DataflowPlan] = None

  override def preparePlan: Unit = {
    generator match {
      case gen @ GeneratorPlan(opList) => {
        /*
         * Nested foreach require special handling: we construct a subplan for the operator list
         * and add our input pipe to the context of the plan.
         */
        val plan = new DataflowPlan(opList, Some(List(inputs.head)))
        // println("--> " + plan.operators.mkString("\n"))

        plan.operators.foreach(op =>
          if (op.isInstanceOf[Generate]) {
            val genOp = op.asInstanceOf[Generate]

            // we extract the input pipes of the GENERATE statements (which are hidden
            // inside the expressions
            val pipes = genOp.findInputPipes(plan)
            // and update the other ends of the pipes accordingly
            pipes.foreach(p => p.producer.addConsumer(p.name, op))
            // we need these pipes only to avoid the removal of disconnected operators

            genOp.parentOp = this
            genOp.setAdditionalPipesFromPlan(plan)
          }
          else if (op.isInstanceOf[ConstructBag]) {
            op.asInstanceOf[ConstructBag].parentOp = Some(this)
          }
        )
        val lastOp = plan.operators.last
        if (lastOp.isInstanceOf[Generate]) {
          lastOp.asInstanceOf[Generate].parentOp = this
          lastOp.constructSchema
        }
        else
          throw new InvalidPlanException("last statement in nested foreach must be a generate: " + lastOp)

        gen.subPlan = plan.operators
        subPlan = Some(plan)
      }
      case _ => {}
    }
  }

  override def resolveReferences(mapping: Map[String, Ref]): Unit = generator match {
    case GeneratorList(exprs) => exprs.foreach(_.expr.resolveReferences(mapping))
    case GeneratorPlan(plan) => {
      // TODO
    }
  }

  override def checkConnectivity: Boolean = {
    def referencedInGenerate(op: Generate, pipe: Pipe): Boolean =
      op.additionalPipes.filter(p => p.name == pipe.name).nonEmpty

    generator match {
      case GeneratorList(expr) => true
      case GeneratorPlan(plan) => {
        var result: Boolean = true
        val genOp = plan.last.asInstanceOf[Generate]
        plan.foreach { op => {
          /*
          // println("check operator: " + op)
          if (!checkSubOperatorConnectivity(op)) {
            println("op: " + op + " : not connected")
            result = false
          }
          */
          if (!op.inputs.forall(p => p.producer != null)) {
            println("op: " + op + " : invalid input pipes: " + op.inputs.mkString(","))
            result = false
          }
          if (!op.outputs.forall(p => p.consumer.nonEmpty || referencedInGenerate(genOp, p))) {
            println("op: " + op + " : invalid output pipes: " + op.outputs.mkString(","))
            result = false
          }
        }
        }
        result
      }
    }
  }


  override def constructSchema: Option[Schema] = {
    generator match {
      case gen@GeneratorList(expr) => {
        val fields = gen.constructFieldList(inputSchema)
        schema = Some(Schema(BagType(TupleType(fields))))
      }
      case GeneratorPlan(_) => {
        val plan = subPlan.get.operators
        // if we have ConstructBag operators in our subplan, we should add schema information
        plan.filter(p => p.isInstanceOf[ConstructBag]).foreach(p => p.asInstanceOf[ConstructBag].parentSchema = inputSchema)
        // we invoke constructSchema for all operators of the subplan
        plan.foreach(op => op.constructSchema)

        /*
         * TODO: expressions in generate can refer to _all_ bags in the subplan
         */
        val genOp = plan.last
        if (genOp.isInstanceOf[Generate]) {
          schema = genOp.schema
          // schema.get.setBagName(outPipeName)
        }
        else
          throw new InvalidPlanException("last statement in nested foreach must be a generate")
      }
    }
    schema
  }

  override def checkSchemaConformance: Boolean = {
    generator match {
      case GeneratorList(expr) => inputSchema match {
        case Some(s) => {
          // if we know the schema we check all named fields
          expr.map(_.expr.traverseAnd(s, Expr.checkExpressionConformance)).foldLeft(true)((b1: Boolean, b2: Boolean) => b1 && b2)
        }
        case None => {
          // if we don't have a schema all expressions should contain only positional fields
          expr.map(_.expr.traverseAnd(null, Expr.containsNoNamedFields)).foldLeft(true)((b1: Boolean, b2: Boolean) => b1 && b2)
        }
      }
      case GeneratorPlan(plan) => {
        subPlan.get.checkSchemaConformance
        true
      }
    }
  }

  /**
   * Looks for an operator in the subplan that produces a bag with the given name.
   *
   * @param name
   * @return
   */
  def findOperatorInSubplan(name: String): Option[PigOperator] = subPlan match {
    case Some(plan) => plan.findOperatorForAlias(name)
    case None => None
  }
  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    generator match {
      case GeneratorList(expr) => s"""FOREACH%${expr}%""" + super.lineageString
      case GeneratorPlan(plan) => s"""FOREACH""" + super.lineageString // TODO: implement lineageString for nested foreach
    }
  }

  def containsFlatten(onBag: Boolean = false): Boolean = {
    val theSchema = inputSchema.orNull
    generator match {
      case GeneratorList(exprs) =>
        if (onBag) {
          exprs.map(g => g.expr.traverseOr(theSchema, Expr.containsFlattenOnBag)).exists(b => b)
        }
        else
          exprs.map(g => g.expr.traverseOr(theSchema, Expr.containsFlatten)).exists(b => b)
      case GeneratorPlan(plan) =>
        false // TODO: what happens if GENERATE contains flatten?
    }
  }
  
  override def toString() = s"""FOREACH { out = ${outPipeNames.mkString(",")} , in = ${inPipeNames.mkString(",")} }
                                |  inSchema = $inputSchema
                                |  outSchema = $schema)""".stripMargin

  override def printOperator(tab: Int): Unit = {
    println(indent(tab) + this.toString())
    generator match {
      case GeneratorList(exprs) => println(indent(tab + 2) + "exprs = " + exprs.mkString(","))
      case GeneratorPlan(_) => subPlan.get.printPlan(tab + 5)
    }
  }
}

