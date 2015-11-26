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

import dbis.pig.backends.BackendManager
import dbis.pig.expr.{Value, Ref}
import dbis.pig.schema.Schema
import java.net.URI

import scala.collection.mutable.Map

/**
 * Load represents the LOAD operator of Pig.
 *
 * @param out the output pipe (relation).
 * @param file the name of the file to be loaded
 * @param loadSchema
 * @param loaderFunc
 * @param loaderParams
 */
case class Load(out: Pipe, 
                var file: URI,
                var loadSchema: Option[Schema] = None,
                loaderFunc: Option[String] = None, //BackendManager.backend.defaultConnector,
                loaderParams: List[String] = null) extends PigOperator {
  _outputs = List(out)
  _inputs = List()
  schema = loadSchema

  override def constructSchema: Option[Schema] = schema

  /**
   * Returns the lineage string describing the sub-plan producing the input for this operator.
   *
   * @return a string representation of the sub-plan.
   */
  override def lineageString: String = {
    s"""LOAD%${file}%""" + super.lineageString
  }

  override def printOperator(tab: Int): Unit = {
    println(indent(tab) + s"LOAD { out = ${outPipeName} }")
    println(indent(tab + 2) + "file = " + file.toString)
    if (loaderFunc.isDefined) {
      println(indent(tab + 2) + "func = " + loaderFunc.get)
    }
    println(indent(tab + 2) + "outSchema = " + schema)
  }

  override def resolveReferences(mapping: Map[String, Ref]): Unit = {
    // we replace only the filename
    if (file.toString.startsWith("$") && mapping.contains(file.toString)) {
      mapping(file.toString) match {
        case Value(v) => {
          val s = v.toString
          if (s(0) == '"')
            file = new URI(s.substring(1, s.length-1))
        }
        case _ => {}
      }
    }
  }

}
