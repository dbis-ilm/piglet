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

import dbis.pig.schema.Schema

/**
 *
 * @param initialOutPipeName the name of the initial output pipe (relation) which is needed to construct the plan, but
 *                           can be changed later.
 * @param initialInPipeName
 * @param opName
 * @param params
 * @param loadSchema
 */
case class StreamOp(override val initialOutPipeName: String, initialInPipeName: String, opName: String, params: Option[List[Ref]] = None,
                    var loadSchema: Option[Schema] = None)
  extends PigOperator(initialOutPipeName, List(initialInPipeName), loadSchema) {
  override def lineageString: String = s"""STREAM%""" + super.lineageString

  override def checkSchemaConformance: Boolean = {
    // TODO
    true
  }

  override def constructSchema: Option[Schema] = {
    // TODO
    schema
  }
}

