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

import dbis.pig.plan.Pipe
import dbis.pig.schema._

import scala.collection.mutable.ArrayBuffer

/**
 * Union represents the UNION operator of Pig.
 *
 * @param initialOutPipeName the name of the output pipe (relation).
 * @param initialInPipeNames the list of names of input pipes.
 */
case class Union(override val initialOutPipeName: String, override val initialInPipeNames: List[String])
  extends PigOperator(initialOutPipeName, initialInPipeNames) {
  override def lineageString: String = {
    s"""UNION%""" + super.lineageString
  }

  override def constructSchema: Option[Schema] = {
    val bagType = (p: Pipe) => p.producer.schema.get.element
    val generalizedBagType = (b1: BagType, b2: BagType) => {
      require(b1.valueType.fields.length == b2.valueType.fields.length)
      val newFields = ArrayBuffer[Field]()
      val fieldPairs = b1.valueType.fields.zip(b2.valueType.fields)
      for ((f1, f2) <- fieldPairs) {
        newFields += Field(f1.name, Types.escalateTypes(f1.fType, f2.fType))
      }
      BagType(b1.s, TupleType(b1.valueType.s, newFields.toArray))
    }

    // case 1: one of the input schema isn't known -> output schema = None
    if (inputs.exists(p => p.producer.schema == None)) {
      schema = None
    }
    else {
      // case 2: all input schemas have the same number of fields
      val s1 = inputs.head.producer.schema.get
      if (! inputs.tail.exists(p => s1.fields.length != p.producer.schema.get.fields.length)) {
        val typeList = inputs.map(p => bagType(p))
        val resultType = typeList.reduceLeft(generalizedBagType)
        schema = Some(new Schema(resultType))
      }
      else {
        // case 3: the number of fields differ
        schema = None
      }
    }
    schema
  }


}

