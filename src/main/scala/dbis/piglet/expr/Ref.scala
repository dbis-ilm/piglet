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
package dbis.piglet.expr

import dbis.piglet.schema.Field

sealed abstract class Ref

case class NamedField(var name: String, lineage: List[String] = List.empty) extends Ref {
//  override def toString = name + (if (lineage.nonEmpty) lineage.mkString("::") else "")
  override def toString = (lineage :+ name).mkString(Field.lineageSeparator)
}

object NamedField {
  def fromString(s: String): NamedField = {
    val split = s.split(Field.lineageSeparator)
    NamedField(split.last, split.init.toList)
  }

  def fromStringList(s: List[String]): NamedField = {
    require(s.nonEmpty, "s has to contain at least one element, the name of the field")
    if (s.length == 1) {
      NamedField(s.head)
    } else {
      fromString(s.mkString(Field.lineageSeparator))
    }
  }
}

case class PositionalField(pos: Int) extends Ref {
  override def toString = "$" + pos
}

case class Value(var v: Any) extends Ref

case class DerefTuple(tref: Ref, component: Ref) extends Ref

case class DerefStreamingTuple(tref: Ref, component: Ref) extends Ref

case class DerefMap(mref: Ref, key: String) extends Ref
