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

import dbis.piglet.plan.InvalidPlanException
import dbis.piglet.schema._
import dbis.piglet.expr.DerefTuple
import dbis.piglet.expr.NamedField
import dbis.piglet.expr.PositionalField
import dbis.piglet.expr.Ref

/**
 * This operator is a pseudo operator used inside a nested FOREACH to construct a new bag from an expression.
 *
 * @param out the output pipe (relation).
 * @param refExpr a reference referring to an expression constructing a relation (bag).
 */
case class ConstructBag(
    private val out: Pipe, 
    refExpr: Ref
  ) extends PigOperator(out) {

  // TODO: what do we need here?
  var parentSchema: Option[Schema] = None

  var parentOp: Option[PigOperator] = None

  override def constructSchema: Option[Schema] = {
    parentSchema match {
      case Some(s) =>
        // first, we determine the field in the schema
        val field = refExpr match {
          case DerefTuple(t, r) => t match {
            case nf@NamedField(n, _) =>
              // Either we refer to the input pipe (inputSchema) ...
              if (parentOp.isDefined && parentOp.get.inPipeName == n)
              // then we create a temporary pseudo field ...
                Field(n, s.element)
              else
              // ... or we refer to a real field of the schema
                s.field(nf)
            case PositionalField(p) => s.field(p)
            case _ => throw InvalidPlanException("unexpected expression in ConstructBag")
          }
          case _ => throw InvalidPlanException("unexpected expression in ConstructBag")
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
        val resSchema = Schema(componentType match {
          case bagType: BagType => bagType
          case _ => BagType(TupleType(Array(Field(componentName, componentType))))
        })
        schema = Some(resSchema)
      case None => None
    }
    schema
  }

  override def toString: String = {
    s"""CONSTRUCT_BAG
       |  out = ${outPipeNames.mkString(",")}
       |  inSchema = $inputSchema
       |  outSchema = $schema
       |  ref = $refExpr""".stripMargin
  }
}