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
package dbis.pig.tools

import scala.sys.process._

class FlinkRun extends Run{
  override def execute(master: String, className: String, jarFile: String){
    val flinkJar = sys.env.get("FLINK_JAR") match {
      case Some(n) => n
      case None => throw new Exception(s"Please set FLINK_JAR to your flink-dist jar file")
    }
    val run = s"java -Dscala.usejavacp=true -cp ${flinkJar}:${jarFile} ${className}"
    println(run)
    run !
  }
}
