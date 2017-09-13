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

import dbis.piglet.expr.{NamedFieldExtractor, RefExprExtractor}
import dbis.piglet.plan.{PipeNameGenerator, DataflowPlan}
import dbis.piglet.schema._
import scala.collection.mutable.ListBuffer
import dbis.piglet.expr.NamedField
import dbis.piglet.expr.PositionalField
import dbis.piglet.expr.Value
import dbis.piglet.expr.DerefStreamingTuple
import dbis.piglet.expr.DerefTuple
import dbis.piglet.expr.DerefMap

/**
 * GENERATE represents the final generate statement inside a nested FOREACH.
 *
 * @param exprs list of generator expressions
 */
case class Generate(exprs: List[GeneratorExpr]) extends PigOperator(List(), List()) {

  var parentOp: Foreach = _

  /**
   * A list of additional pipes from the context (i.e., the nested
   * statements) and are referenced in the generate expression.
   */
  var additionalPipes = List[Pipe]()

  def copyPipes(op: Generate): Unit = {
    _inputs = op._inputs
    _outputs = op._outputs
    additionalPipes = op.additionalPipes
  }

  /**
   * Check all generate expressions and derive from the NamedField objects all referenced
   * additional pipes. Candidate pipes are pipes which are output pipes of other operators.
   *
   * @param plan the dataflow plan representing the nested statements
   */
  def setAdditionalPipesFromPlan(plan: DataflowPlan): Unit = {
    if (additionalPipes.isEmpty) {
      val traverse = new NamedFieldExtractor
      exprs.foreach(e => e.expr.traverseAnd(null, traverse.collectNamedFields))
      additionalPipes = traverse.fields.map(field => plan.findOperatorForAlias(field.name) match {
        case Some(op) => Pipe(field.name, op, List(this))
        case None => Pipe("")
      }).filter(p => p.name != "").toList
    }
  }

  /**
   * Check all generate expressions and derive from the NamedField objects all possible
   * input pipes. Input pipes are pipes which are output pipes of other operators.
   *
   * @param plan The
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

  override def constructSchema: Option[Schema] = {
    val fields = constructFieldList(exprs)
    schema = Some(Schema(BagType(TupleType(fields))))
    schema
  }

  /**
   * Collect the list of fields available for this operator.
   *
   * @return a pair of lists
   */
  def collectInputFields(): List[Field] = {
    // we collect a list of fields from our parentSchema
    val fieldList = ListBuffer[Field]()
    if (parentOp != null)
      parentOp.inputSchema match {
        case Some(s) => fieldList ++= s.fields
        case None =>
      }

    fieldList.toList
  }

  override def checkSchemaConformance: Boolean = {
    // println("GENERATE.inputSchema = " + parentOp.inputSchema)
    require(parentOp.inputSchema.isDefined)
    val schema = parentOp.inputSchema.get

    // we have to extract all RefExprs
    val traverse = new RefExprExtractor
    exprs.foreach(e => e.expr.traverseAnd(null, traverse.collectRefExprs))
    val refExprs = traverse.exprs.toList

    val res = refExprs.map(rex => rex.r match {
      case nf@NamedField(n, _) =>
        // is n just a simple field in our input?
        schema.indexOfField(nf) != -1 ||
        // otherwise we check whether n refers to a pipe
        additionalPipes.exists(p => p.name == n)
      case PositionalField(p) => p < schema.fields.length
      case Value(v) => true // okay
      case DerefStreamingTuple(r1, r2) => true // TODO: is r1 a valid ref?
      case DerefTuple(r1, r2) => true // TODO: is r1 a valid ref?
      case DerefMap(r1, r2) => true // TODO: is r1 a valid ref?
    })
    ! res.contains(false)
  }

  // TODO: eliminate replicated code
  def constructFieldList(exprs: List[GeneratorExpr]): Array[Field] = {
    // println("GENERATE.constructFieldList: " + exprs.mkString(","))
    val inSchema = Some(new Schema(BagType(new TupleType(collectInputFields().toArray))))
    // println("inSchema = " + inSchema)
    exprs.map(e => {
      e.alias match {
        // if we have an explicit schema (i.e. a field) then we use it
        case Some(f) =>
          if (f.fType == Types.ByteArrayType) {
            // if the type was only bytearray, we should check the expression if we have a more
            // specific type
            val res = e.expr.resultType(inSchema)
            Field(f.name, res)
          }
          else
            f
        // otherwise we take the field name from the expression and
        // the input schema
        case None =>
          var res = e.expr.resultType(inSchema)
          // println(" ===> " + e + " type = " + res)
          Field("", res)
      }
    }).toArray
  }

  override def toString =
    s"""GENERATE
       |  out = ${outPipeNames.mkString(",")}
       |  in = ${inPipeNames.mkString("m")}
       |  inSchema = ${parentOp.schema}
       |  outSchema = $schema
       |  expr = ${exprs.mkString(",")}""".stripMargin


}

