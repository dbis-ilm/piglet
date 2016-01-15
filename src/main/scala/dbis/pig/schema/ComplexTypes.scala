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
 */
abstract class ComplexType extends java.io.Serializable with PigType {
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
 */
case class TupleType(var fields: Array[Field],var className: String = "") extends ComplexType {
  override def tc = TypeCode.TupleType
  override def name = "tuple"

  override def encode: String = s"(${fields.map(f => f.name + ":" + f.fType.encode).mkString})"

  override def equals(that: Any): Boolean = that match {
    case TupleType(f, _) => this.fields.deep == f.deep
    case _ => false
  }

  /**
   * Returns a string representation of the type object.
   *
   * @return the string repesentation
   */
  override def toString = s"${name}(" + fields.mkString(",") + ")"

  override def descriptionString = "(" + fields.mkString(", ") + ")"

  def plainDescriptionString = fields.mkString(", ")

  override def typeOfComponent(name: String): PigType = fields.find(f => f.name == name).head.fType
  override def typeOfComponent(pos: Int): PigType = fields(pos).fType
}

object TupleType {
  def apply(fields: Array[Field]) = {
    val t = new TupleType(fields)
    registerType(t)
  }

  def registerType(tt: TupleType): TupleType = {
    val schema = Schema.registerSchema(Schema(BagType(tt)))
    tt.className = schema.className
    tt
  }

}


/**
 * A type for representing Pig bags consisting of a value type (a tuple type) and an
 * optional type name.
 *
 * @param valueType the tuple type representing the element of the bag
 */
case class BagType(var valueType: TupleType) extends ComplexType {
  override def tc = TypeCode.BagType
  override def name = "bag"

  /**
   * Returns a string representation of the type object.
   *
   * @return the string repesentation
   */
  override def toString = s"${name}{${valueType}}"

  override def encode: String = s"{${valueType.encode}}"

  override def descriptionString = "{" + valueType.plainDescriptionString + "}"
  override def typeOfComponent(name: String): PigType = valueType.typeOfComponent(name)
  override def typeOfComponent(pos: Int): PigType = valueType.typeOfComponent(pos)
}

/**
 * A type of representing Pig maps consisting of a value type (arbitrary Pig type) and
 * an optional type name.
 *
 * @param valueType the Pig type used for values of the map
 */
case class MapType(var valueType: PigType) extends ComplexType {
  override def tc = TypeCode.MapType
  override def name = "map"

  override def encode: String = s"[${valueType.encode}]"

  override def descriptionString = "[" + valueType.descriptionString + "]"
  override def typeOfComponent(name: String): PigType = valueType
  override def typeOfComponent(pos: Int): PigType = valueType
}

object MatrixRep extends Enumeration {
  type MatrixRep = Value
  val DenseMatrix, SparseMatrix = Value
}

import dbis.pig.schema.MatrixRep._

case class MatrixType(valueType: PigType, rows: Int, cols: Int, rep: MatrixRep = DenseMatrix) extends ComplexType {
  require(valueType.tc == TypeCode.IntType || valueType.tc == TypeCode.DoubleType)

  override def tc = TypeCode.MatrixType
  override def name = s"${if (rep == DenseMatrix) "d" else "s"}${if (valueType.tc == TypeCode.IntType) "i" else "d"}matrix($rows,$cols)"

  override def typeOfComponent(name: String): PigType = valueType
  override def typeOfComponent(pos: Int): PigType = { require(pos == 0); valueType }

  override def encode: String = s"m[${valueType.encode}$rows,$cols]"

  override def descriptionString = name
}