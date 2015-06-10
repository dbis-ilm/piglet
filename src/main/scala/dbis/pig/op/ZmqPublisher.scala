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
  * ZmqPublisher represents the ZMQ_PUBLISHER operator of Pig.
  *
  * @param initialInPipeName the name of the input pipe
  * @param addr the zmq address to publish to
  */
case class ZmqPublisher(initialInPipeName: String, addr: String) extends PigOperator("", initialInPipeName) {

  /** 
    * Returns the lineage string describing the sub-plan producing the input for this operator.
    *
    * @return a string representation of the sub-plan.
    */
  override def lineageString: String = { 
    s"""ZMQ_PUBLISHER%${addr}%""" + super.lineageString
  }
}