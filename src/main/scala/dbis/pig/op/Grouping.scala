/*
 * Copyright (c) 2015 The Piglet team,
 *                    All Rights Reserved.
 *
 * This file is part of the Piglet package.
 *
 * PipeFabric is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License (GPL) as
 * published by the Free Software Foundation; either version 2 of
 * the License, or (at your option) any later version.
 *
 * This package is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; see the file LICENSE.
 * If not you can find the GPL at http://www.gnu.org/copyleft/gpl.html
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
case class Grouping(override val initialOutPipeName: String, initialInPipeName: String, groupExpr: GroupingExpression)
  extends PigOperator(initialOutPipeName, initialInPipeName) {

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""GROUPBY%${groupExpr}%""" + super.lineageString
  }

  override def constructSchema: Option[Schema] = {
    // val inputSchema = inputs.head.producer.schema
    // tuple(group: typeOfGroupingExpr, in:bag(inputSchema))
    val inputType = inputSchema match {
      case Some(s) => s.element.valueType
      case None => TupleType("", Array(Field("", Types.ByteArrayType)))
    }
    val groupingType = Types.IntType
    val fields = Array(Field("group", groupingType),
      Field(inputs.head.name, BagType("", inputType)))
    schema = Some(new Schema(new BagType("", new TupleType("", fields))))
    schema
  }

  override def checkSchemaConformance: Boolean = {
    schema match {
      case Some(s) => {
        // if we know the schema we check all named fields
        groupExpr.keyList.filter(_.isInstanceOf[NamedField]).exists(f => s.indexOfField(f.asInstanceOf[NamedField].name) != -1)
      }
      case None => {
        // if we don't have a schema all expressions should contain only positional fields
        groupExpr.keyList.map(_.isInstanceOf[NamedField]).exists(b => b)
      }
    }
  }
}


