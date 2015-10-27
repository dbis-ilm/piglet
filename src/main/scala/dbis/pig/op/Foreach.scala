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

import dbis.pig.plan.{InvalidPlanException, DataflowPlan}
import dbis.pig.schema._

import scala.collection.mutable.{ListBuffer, Map}


/**
 * A trait for the GENERATE part of a FOREACH operator.
 */
trait ForeachGenerator {}

/**
 * GeneratorExpr represents a single expression of a Generator.
 *
 * @param expr
 * @param alias
 */
case class GeneratorExpr(expr: ArithmeticExpr, alias: Option[Field] = None)

/**
 * GeneratorList implements the ForeachGenerator trait and is used to represent
 * the FOREACH ... GENERATE operator.
 *
 * @param exprs
 */
case class GeneratorList(var exprs: List[GeneratorExpr]) extends ForeachGenerator

/**
 * GeneratorPlan implements the ForeachGenerator trait and is used to represent
 * a nested FOREACH.
 *
 * @param subPlan
 */
case class GeneratorPlan(var subPlan: List[PigOperator]) extends ForeachGenerator

/**
 * Foreach represents the FOREACH operator of Pig.
 *
 * @param out the output pipe (relation).
 * @param in the input pipe
 * @param generator the generator (a list of expressions or a subplan)
 * @param windowMode ???
 */
case class Foreach(out: Pipe,
                   in: Pipe,
                   var generator: ForeachGenerator,
                   var windowMode: Boolean = false) extends PigOperator {
  _outputs = List(out)
  _inputs = List(in)

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

        plan.operators.foreach(op => if (op.isInstanceOf[Generate]) {
          // we extract the input pipes of the GENERATE statements (which are hidden
          // inside the expressions
          val pipes = op.asInstanceOf[Generate].findInputPipes(plan)
          // and update the other ends of the pipes accordingly
          pipes.foreach(p => p.producer.addConsumer(p.name, op))
        }
        )
        val genOp = plan.operators.last
        if (genOp.isInstanceOf[Generate]) {
          genOp.asInstanceOf[Generate].parentOp = this
        }
        else
          throw new InvalidPlanException("last statement in nested foreach must be a generate")

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
    generator match {
      case GeneratorList(expr) => true
      case GeneratorPlan(plan) => {
        var result: Boolean = true
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
          if (!op.outputs.forall(p => p.consumer.nonEmpty)) {
            println("op: " + op + " : invalid output pipes: " + op.outputs.mkString(","))
            result = false
          }
        }
        }
        result
      }
    }
  }

  def constructFieldList(exprs: List[GeneratorExpr]): Array[Field] =
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
        case None => val res = e.expr.resultType(inputSchema); Field("", res)
      }
    }).toArray

  override def constructSchema: Option[Schema] = {
    if (inputSchema.isDefined)
      inputSchema.get.setBagName(inPipeName)

    generator match {
      case GeneratorList(expr) => {
        val fields = constructFieldList(expr)

        schema = Some(new Schema(new BagType(new TupleType(fields), outPipeName)))
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
          schema.get.setBagName(outPipeName)
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
}

class NamedFieldExtractor {
  val fields = ListBuffer[NamedField]()

  def collectNamedFields(schema: Schema, ex: Expr): Boolean = ex match {
    case RefExpr(r) => r match {
      case NamedField(n, _) => fields += r.asInstanceOf[NamedField]; true
      case _ => true
    }
    case _ => true
  }
}

class RefExprExtractor {
  val exprs = ListBuffer[RefExpr]()

  def collectRefExprs(schema: Schema, ex: Expr): Boolean = ex match {
    case RefExpr(r) => exprs += ex.asInstanceOf[RefExpr]; true
    case _ => true
  }
}

class FuncExtractor {
  val funcs = ListBuffer[Func]()

  def collectFuncExprs(schema: Schema, ex: Expr): Boolean = ex match {
    case Func(f, params) => funcs += ex.asInstanceOf[Func]; true
    case _ => true
  }
}

/**
 * GENERATE represents the final generate statement inside a nested FOREACH.
 *
 * @param exprs list of generator expressions
 */
case class Generate(exprs: List[GeneratorExpr]) extends PigOperator {
  _outputs = List()
  _inputs = List()

  var parentOp: Foreach = null

  def copyPipes(op: Generate): Unit = {
    _inputs = op._inputs
    _outputs = op._outputs
  }

  /**
   * Check all generate expressions and derive from the NamedField objects all possible
   * input pipes. Input pipes are pipes which are output pipes of other operators.
   *
   * @param plan
   */
  def findInputPipes(plan: DataflowPlan): List[Pipe] = {
    val traverse = new NamedFieldExtractor
    exprs.foreach(e => e.expr.traverseAnd(null, traverse.collectNamedFields))
    val newInput: List[Pipe] = traverse.fields.map(field => plan.findOperatorForAlias(field.name) match {
      case Some(op) => Pipe(field.name, op, List(this))
      case None => Pipe("")
    }).filter(p => p.name != "").toList
   _inputs =  newInput
    _inputs
  }

  // TODO: what do we need here?
  override def constructSchema: Option[Schema] = {
    val fields = constructFieldList(exprs)
    schema = Some(new Schema(new BagType(new TupleType(fields))))
    schema
  }

  override def checkSchemaConformance: Boolean = {
    // we have to extract all RefExprs
    val traverse = new RefExprExtractor
    exprs.foreach(e => e.expr.traverseAnd(null, traverse.collectRefExprs))

    // we collect a list of fields of all input schemas
    val fieldList = ListBuffer[Field]()
    inputs.foreach(p => p.inputSchema match {
      case Some(s) => fieldList ++= s.fields
      case None => {}
    })
    val res = traverse.exprs.map(rex => rex.r match {
      case NamedField(n, _) => fieldList.exists(_.name == n)
      case PositionalField(p) => if (fieldList.isEmpty) p == 0 else p < fieldList.length
      case Value(v) => true // okay
      case DerefStreamingTuple(r1, r2) => true // TODO: is r1 a valid ref?
      case DerefTuple(r1, r2) => true // TODO: is r1 a valid ref?
      case DerefMap(r1, r2) => true // TODO: is r1 a valid ref?
    })
    ! res.contains(false)
  }

  // TODO: eliminate replicated code
  def constructFieldList(exprs: List[GeneratorExpr]): Array[Field] =
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
        case None => val res = e.expr.resultType(inputSchema); Field("", res)
      }
    }).toArray
}

/**
 * This operator is a pseudo operator used inside a nested FOREACH to construct a new bag from an expression.
 *
 * @param out the output pipe (relation).
 * @param refExpr a reference referring to an expression constructing a relation (bag).
 */
case class ConstructBag(out: Pipe, refExpr: Ref) extends PigOperator {
  _outputs = List(out)
  _inputs = List()

  // TODO: what do we need here?
  var parentSchema: Option[Schema] = None

  override def constructSchema: Option[Schema] = {
    parentSchema match {
      case Some(s) => {
        // first, we determine the field in the schema
        val field = refExpr match {
          case DerefTuple(t, r) => t match {
            case nf @ NamedField(n, _) => {
              if (s.element.name == n)
                Field(n, s.element)
              else
                s.field(nf)
            }
            case PositionalField(p) => s.field(p)
            case _ => throw new InvalidPlanException("unexpected expression in ConstructBag")
          }
          case _ => throw new InvalidPlanException("unexpected expression in ConstructBag")
        }
        // 2. we extract the type (which should be a BagType, MapType or TupleType)
        if (!field.fType.isInstanceOf[ComplexType])
          throw InvalidPlanException("invalid expression in ConstructBag")
        val fieldType = field.fType.asInstanceOf[ComplexType]

        val (componentName, componentType) = refExpr match {
          case DerefTuple(t, r) => r match {
            case NamedField(n, _) => (n, fieldType.typeOfComponent(n))
            case PositionalField(p) => ("", fieldType.typeOfComponent(p))
            case _ => throw InvalidPlanException("unexpected expression in ConstructBag")
          }
          case _ => throw InvalidPlanException("unexpected expression in ConstructBag")
        }
        // construct a schema from the component type
//        val resSchema = new Schema(new BagType(new TupleType(Array(Field(componentName, componentType))), outPipeName))
        val resSchema = new Schema(if (componentType.isInstanceOf[BagType])
          componentType.asInstanceOf[BagType]
        else
          new BagType(new TupleType(Array(Field(componentName, componentType))), outPipeName))
        schema = Some(resSchema)

      }
      case None => None
  }
    schema
  }
}
