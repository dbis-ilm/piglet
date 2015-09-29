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

package dbis.pig.schema

case class Field(name: String, fType: PigType = Types.ByteArrayType, lineage: List[String] = List.empty) {
  override def toString = s"${nameWithLineage}: ${fType.descriptionString}"
  def isBagType = fType.isInstanceOf[BagType]

  def nameWithLineage: String = {
    if (lineage.isEmpty) {
      name
    } else {
      lineage :+ name mkString(Field.lineageSeparator)
    }
  }
}

object Field {
  final val lineageSeparator = "::"
}

/**
 * A base class for complex types, i.e. bag, tuple, and map.
 * Basically, it provides an interface to retrieve the types of individual components.
 *
 * @param s the type name
 */
abstract class ComplexType(s: String) extends PigType(s) {
  /**
   * Returns the type of the component with the given field name.
   *
   * @param name the field name
   * @return the name of this field
   */
  def typeOfComponent(name: String): PigType

  /**
   * Returns the type of the component of the field at the given position.
   *
   * @param pos the position of the field
   * @return the name of this field
   */
  def typeOfComponent(pos: Int): PigType
}

/**
 * A type for Pig tuples consisting of an array of fields (name + types) and an
 * optional type name.
 *
 * @param fields the list of fields representing the tuple components.
 * @param s the type name
 */
case class TupleType(var fields: Array[Field], s: String = "") extends ComplexType(s) {
  override def equals(that: Any): Boolean = that match {
    case TupleType(fields, name) => this.name == name && this.fields.deep == fields.deep
    case _ => false
  }

  /**
   * Returns a string representation of the type object.
   *
   * @return the string repesentation
   */
  override def toString = "TupleType(" + name + "," + fields.mkString(",") + ")"

  override def descriptionString = "(" + fields.mkString(", ") + ")"

  def plainDescriptionString = fields.mkString(", ")

  override def typeOfComponent(name: String): PigType = fields.find(f => f.name == name).head.fType
  override def typeOfComponent(pos: Int): PigType = fields(pos).fType
}

/**
 * A type for representing Pig bags consisting of a value type (a tuple type) and an
 * optional type name.
 *
 * @param valueType the tuple type representing the element of the bag
 * @param s the type name
 */
case class BagType(var valueType: TupleType, s: String = "") extends ComplexType(s) {
  override def descriptionString = "{" + valueType.plainDescriptionString + "}"
  override def typeOfComponent(name: String): PigType = valueType.typeOfComponent(name)
  override def typeOfComponent(pos: Int): PigType = valueType.typeOfComponent(pos)
}

/**
 * A type of representing Pig maps consisting of a value type (arbitrary Pig type) and
 * an optional type name.
 *
 * @param valueType the Pig type used for values of the map
 * @param s the type name
 */
case class MapType(var valueType: PigType, s: String = "") extends ComplexType(s) {
  override def descriptionString = "[" + valueType.descriptionString + "]"
  override def typeOfComponent(name: String): PigType = valueType
  override def typeOfComponent(pos: Int): PigType = valueType
}