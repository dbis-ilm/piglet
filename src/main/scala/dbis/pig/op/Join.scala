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

import dbis.pig.schema.{TupleType, BagType, Field, Schema}

import scala.collection.mutable.ArrayBuffer

/**
 * Join represents the multiway JOIN operator of Pig.
 *
 * @param outPipeName the name of the output pipe (relation).
 * @param inPipeNames the list of names of input pipes.
 * @param fieldExprs  list of key expressions (list of keys) used as join expressions.
 */
case class Join(override val outPipeName: String, override val inPipeNames: List[String], val fieldExprs: List[List[Ref]])
  extends PigOperator(outPipeName, inPipeNames) {
  override def lineageString: String = {
    s"""JOIN%""" + super.lineageString
  }

  override def constructSchema: Option[Schema] = {
    val newFields = ArrayBuffer[Field]()
    inputs.foreach(p => p.producer.schema match {
      case Some(s) => newFields ++= s.fields
      case None => ???
    })
    schema = Some(new Schema(BagType("", TupleType("", newFields.toArray))))
    schema
  }

}

