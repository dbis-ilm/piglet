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
case class Schema(var element: BagType) {
  def setBagName(s: String) =  element.name = s

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
      throw SchemaException("schema type isn't a bag of tuples")
    val tupleType = element.valueType.asInstanceOf[TupleType]
    tupleType.fields(pos)
  }

  /**
   * Returns the field with the given name.
   *
   * @param name the name of the field
   * @return the field definition
   */
  def field(name: String): Field = {
    val idx = indexOfField(name)
    if (idx == -1) throw SchemaException("unkown field '" + name + "' in "+ this)
    field(idx)
  }

  def fields: Array[Field] = {
    if (! element.valueType.isInstanceOf[TupleType])
      throw new SchemaException("schema type isn't a bag of tuples")
    val tupleType = element.valueType.asInstanceOf[TupleType]
    tupleType.fields
  }

  override def toString = "Schema(" + element.toString + "," + element.name + ")"
}
