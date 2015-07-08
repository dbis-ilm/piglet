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

import dbis.pig.schema._


/**
 * Represents the grouping expression for the Grouping operator.
 *
 * @param keyList a list of keys used for grouping
 */
case class GroupingExpression(val keyList: List[Ref])

/**
 * Grouping represents the GROUP ALL / GROUP BY operator of Pig.
 *
 * @param initialOutPipeName the name of the output pipe (relation).
 * @param initialInPipeName the name of the input pipe
 * @param groupExpr the expression (a key or a list of keys) used for grouping
 */
case class Grouping(out: Pipe, in: Pipe, groupExpr: GroupingExpression) extends PigOperator {
  _outputs = List(out)
  _inputs = List(in)

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""GROUPBY%${groupExpr}%""" + super.lineageString
  }

  override def constructSchema: Option[Schema] = {
    // tuple(group: typeOfGroupingExpr, in:bag(inputSchema))
    val inputType = inputSchema match {
      case Some(s) => s.element.valueType
      case None => TupleType(Array(Field("", Types.ByteArrayType)))
    }
    val groupingType = Types.IntType
    val fields = Array(Field("group", groupingType),
      Field(inputs.head.name, BagType(inputType)))
    schema = Some(new Schema(new BagType(new TupleType(fields), outPipeName)))
    schema
  }

  override def checkSchemaConformance: Boolean = {
    inputSchema match {
      case Some(s) => {
        // if we know the schema we check all named fields
        ! groupExpr.keyList.filter(_.isInstanceOf[NamedField]).exists(f => s.indexOfField(f.asInstanceOf[NamedField].name) == -1)
      }
      case None => {
        // if we don't have a schema all expressions should contain only positional fields
        ! groupExpr.keyList.map(_.isInstanceOf[NamedField]).exists(b => b)
      }
    }
  }
}


