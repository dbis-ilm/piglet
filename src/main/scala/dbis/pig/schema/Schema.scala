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
package dbis.pig.schema

/**
 * An exception indicating failures in schema handling.
 *
 * @param msg a message describing the exeption.
 */
case class SchemaException(private val msg: String) extends Exception(msg)

/**
 * A schema describes the structure of the output relation of an operator.
 *
 * @param element the type definition - in most cases a bag of tuples
 *
 */
case class Schema(val element: BagType) {
  /**
   * Returns the index of the field in the schema.
   *
   * @param name the name of the field
   * @return the position in the field list
   */
  def indexOfField(name: String) : Int = {
    if (! element.valueType.isInstanceOf[TupleType])
      throw new SchemaException("schema type isn't a bag of tuples")
    val tupleType = element.valueType.asInstanceOf[TupleType]
    tupleType.fields.indexWhere(_.name == name)
  }

  /**
   * Returns the field at the given position.
   *
   * @param pos the position of the field in the schemas field list
   * @return the field definition
   */
  def field(pos: Int): Field = {
    if (! element.valueType.isInstanceOf[TupleType])
      throw new SchemaException("schema type isn't a bag of tuples")
    val tupleType = element.valueType.asInstanceOf[TupleType]
    tupleType.fields(pos)
  }

  /**
   * Returns the field with the given name.
   *
   * @param name the name of the field
   * @return the field definition
   */
  def field(name: String): Field = field(indexOfField(name))

  def fields: Array[Field] = {
    if (! element.valueType.isInstanceOf[TupleType])
      throw new SchemaException("schema type isn't a bag of tuples")
    val tupleType = element.valueType.asInstanceOf[TupleType]
    tupleType.fields
  }
}
