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

import dbis.pig.op.NamedField
import scala.collection.mutable.Map

/**
 * An exception indicating failures in schema handling.
 *
 * @param msg a message describing the exception.
 */
case class SchemaException(private val msg: String) extends Exception(msg)

/**
 * An exception indicating that a field name is ambiguous
 *
 * @param msg a message describing the exception
 */
case class AmbiguousFieldnameException(private val msg: String) extends  Exception(msg)

/**
 * A schema describes the structure of the output relation of an operator.
 * We assume that the element type is a bag of tuple types.
 *
 * @param element the type definition - in most cases a bag of tuples
 * @param className a unique name for the schema which is used to generate classes
 *                  representing tuples. The className is assigned later.
 *
 */
case class Schema(var element: BagType, var className: String = "") {

  def setBagName(s: String): Unit =  {} // element.name = s

  /**
   * Returns a compact representation of the schema which is used to compare two
   * schemas for equality.
   *
   * @return a string representation
   */
  def schemaCode(): String = element.encode

  /**
   * Returns the index of the field in the schema.
   *
   * @param name the name of the field
   * @return the position in the field list
   */
  @throws[SchemaException]("if the element type of the schema isn't a bag of tuples")
  @throws[AmbiguousFieldnameException]("if the NamedField doesn't contain enough information to find the index " +
    "unambiguously")
  def indexOfField(name: String) : Int = indexOfField(NamedField(name))

  /**
   * Returns the index of the field in the schema.
   *
   * @param nf A [[dbis.pig.op.NamedField]] object describing the field
   * @return the position in the field list
   */
  @throws[SchemaException]("if the element type of the schema isn't a bag of tuples")
  @throws[AmbiguousFieldnameException]("if the NamedField doesn't contain enough information to find the index " +
    "unambiguously")
  def indexOfField(nf: NamedField): Int = {
    if (! element.valueType.isInstanceOf[TupleType])
      throw new SchemaException("schema type isn't a bag of tuples")
    val tupleType = element.valueType
    tupleType.fields.count(_.name == nf.name) match {
      case 0 => -1
      case 1 =>
        // There's only one field that has the requested name but it could still happen that the lineage information
        // doesn't match
        val idx = tupleType.fields.indexWhere(_.name == nf.name)
        if (nf.lineage.isEmpty) {
          // If the NamedField doesn't have lineage information and there's only one possible field, return its index
          idx
        } else {
          val field = tupleType.fields(idx)
          if (field.lineage == nf.lineage) {
            idx
          } else {
            -1
          }
        }
      case _ =>
        val possibleField = tupleType.fields.filter(f => nf.name == f.name && nf.lineage == f.lineage)
        if (possibleField.length == 1) {
          tupleType.fields.indexOf(possibleField.head)
        } else {
          throw new AmbiguousFieldnameException(s"""
          | there is more than one field called ${nf.name} in $this
          | and ${nf.lineage} was not enough to disambiguate it""".stripMargin)
        }
    }
  }

  /**
   * Returns the field at the given position.
   *
   * @param pos the position of the field in the schemas field list
   * @return the field definition
   */
  @throws[SchemaException]("if the element type of the schema isn't a bag of tuples")
  def field(pos: Int): Field = {
    if (! element.valueType.isInstanceOf[TupleType])
      throw SchemaException("schema type isn't a bag of tuples")
    val tupleType = element.valueType
    tupleType.fields(pos)
  }

  /**
   * Returns the field with the given name.
   *
   * @param name the name of the field
   * @return the field definition
   */
  @throws[SchemaException]("if the element type of the schema isn't a bag of tuples")
  @throws[SchemaException]("if the schema doesn't contain a field with the given name")
  @throws[AmbiguousFieldnameException]("if the NamedField doesn't contain enough information to find the index " +
    "unambiguously")
  def field(name: String): Field = {
    val idx = indexOfField(name)
    if (idx == -1) throw SchemaException("unknown field '" + name + "' in "+ this)
    field(idx)
  }

  /**
   * Returns the field corresponding to the given [[dbis.pig.op.NamedField]]
   * @param nf
   * @return
   */
  @throws[SchemaException]("if the element type of the schema isn't a bag of tuples")
  @throws[SchemaException]("if the schema doesn't contain a field with the given name")
  @throws[AmbiguousFieldnameException]("if the NamedField doesn't contain enough information to find the index " +
    "unambiguously")
  def field(nf: NamedField): Field = {
    val idx = indexOfField(nf)
    if (idx == -1) throw SchemaException("unknown field '" + nf.name + "' in "+ this)
    field(idx)
  }

  /**
   * Returns an array of all fields assuming that the schema type is a bag of tuples.
   *
   * @return the array of fields of the underlying tuple type.
   */
  @throws[SchemaException]("if the element type of the schema isn't a bag of tuples")
  def fields: Array[Field] = {
    if (! element.valueType.isInstanceOf[TupleType])
      throw SchemaException("schema type isn't a bag of tuples")
    val tupleType = element.valueType
    tupleType.fields
  }

  /**
   * Returns a string representation of the schema.
   *
   * @return the string representation
   */
  override def toString = "Schema(" + element.toString + "," + element.name + ")"
}

/**
 * Companion object for class Schema. The main purpose is to provide a convenient
 * constructor as well as a mechanism to assign unique names for the generated
 * schema classes where different schema instances with the same structure have
 * the same name. We achieve this by collecting all created schemas in a Map where
 * the schema code acts as the key.
 */
object Schema {
  private val schemaSet = Map[String, Schema]()
  private var cnt = 0

  /**
   * Increments the counter used for creating unique class names
   * and returns the current value.
   *
   * @return the next value of the counter
   */
  def nextCounter(): Int = { cnt += 1; cnt }

  def apply(b: BagType) = {
    val s = new Schema(b)
    registerSchema(s)
  }

  def apply(fields: Array[Field]) = {
    val s = new Schema(BagType(TupleType(fields)))
    registerSchema(s)
  }

  /**
   * Clears the map of all schemas and resets the counter.
   */
  def init(): Unit = {
    schemaSet.clear()
    cnt = 0
  }

  /**
   * Registers a schema by inserting it into the Map and
   * deriving a unique class name. If a schema with the
   * same structure already exists then it is just returned
   * without creating a new one.
   *
   * @param schema the schema to be registered
   * @return either a new schema or an already existing schema with
   *         the same structure
   */
  def registerSchema(schema: Schema): Schema = {
    val code = schema.schemaCode
    if (schemaSet.contains(code))
      schemaSet(code)
    else {
      schema.className = s"t${nextCounter}"
      schemaSet += code -> schema
      schema
    }
  }
}