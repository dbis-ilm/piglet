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


object OrderByDirection extends Enumeration {
  type OrderByDirection = Value
  val AscendingOrder, DescendingOrder = Value
}

import dbis.pig.op.OrderByDirection._

/**
 *
 * @param field
 * @param dir
 */
case class OrderBySpec(field: Ref, dir: OrderByDirection)

/**
 *
 * @param initialOutPipeName the name of the initial output pipe (relation) which is needed to construct the plan, but
 *                           can be changed later.
 * @param initialInPipeName
 * @param orderSpec
 */
case class OrderBy(override val initialOutPipeName: String, initialInPipeName: String, orderSpec: List[OrderBySpec])
  extends PigOperator(initialOutPipeName, initialInPipeName) {
  override def lineageString: String = s"""ORDERBY%""" + super.lineageString

  override def checkSchemaConformance: Boolean = {
    schema match {
      case Some(s) => {
        // if we know the schema we check all named fields
        // TODO
      }
      case None => {
        // if we don't have a schema then the OrderBySpec list should contain only positional fields
        orderSpec.filter(s => s.field match {
          case PositionalField(n) => true
          case _ => false
        }).size == orderSpec.size
      }
    }
    true
  }
}