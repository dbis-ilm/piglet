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
package dbis.piglet.schema

/**
 * An exception indicating failures in schema handling.
 *
 * @param msg a message describing the exeption.
 */
case class TypeException(msg: String) extends Exception(msg)

/**
 * An enumeration of possible primitive type codes.
 */
object TypeCode extends Enumeration {
  type TypeCode = Value
  val AnyType, IntType, LongType, FloatType, BooleanType, DoubleType,
  ByteArrayType, CharArrayType,
  TupleType, MapType, BagType, MatrixType,STObject, IndexType = Value
}

import dbis.piglet.schema.TypeCode._

/**
 * The base class for all Pig types.
 */
trait PigType {
  def name: String
  def tc: TypeCode
  def descriptionString: String
  def plainDescriptionString: String = descriptionString
  def encode: String
}

/**
 * The base class for all primitive types (int, float, double, ...).
 *
 * @param name the name of the type.
 * @param tc the typecode representing this type.
 */
case class SimpleType(name: String, tc: TypeCode) extends java.io.Serializable with PigType {
  override def descriptionString = name

  override def encode: String = tc match {
    case TypeCode.AnyType => "?"
    case TypeCode.IntType => "i"
    case TypeCode.LongType => "l"
    case TypeCode.FloatType => "f"
    case TypeCode.BooleanType => "b"
    case TypeCode.DoubleType => "d"
    case TypeCode.CharArrayType => "c"
    case TypeCode.ByteArrayType => "a"
    case _ => "_"
  }

  /**
    * Ask the type system for the number of bytes that each type requires.
    *
    * Primitive types require a fixed number of bytes (e.g., Int = 4 bytes).
    * Other types, such as arrays depend on the number of elements and thus,
    * cannot directly be decided here

    * @return Returns a number > 0 if the type size can be decided here, or
    *         <= 0 for other data types that need further calculations
    */
  lazy val numBytes = tc match {
    case TypeCode.BooleanType => 1
    case TypeCode.IntType => 4
    case TypeCode.FloatType => 4
    case TypeCode.LongType => 8
    case TypeCode.DoubleType => 8
    case _ => -1
  }
}

case class STObjectType() extends PigType with java.io.Serializable {

  override def tc = TypeCode.STObject
//  override def name = "Geometry"
  override def name = "STObject"

  override def encode: String = "g"

  override def descriptionString = name
}

/**
 * An object with some helper functions for type handling.
 */
object Types {
  /**
   * Returns the priority of the given type which is used for determining
   * compatibility between two types.
   *
   * @param t the Pig type
   * @return the priority value (between 0=lowest ... 10=highest)
   */
  def typePriority(t: PigType): Int = t match {
    // double > float > long > int > bytearray
    case ByteArrayType => 0
    case IntType => 1
    case LongType => 2
    case FloatType => 3
    case DoubleType => 4
    // tuple|bag|map|chararray > bytearray
    case CharArrayType => 10
    case _ => 20
  }

  def escalateTypes(t1: PigType, t2: PigType): PigType = {
    if (t1 == t2)
      t1
    else if (!typeCompatibility(t1, t2))
      Types.ByteArrayType
    else {
      val prio1 = typePriority(t1)
      val prio2 = typePriority(t2)
      Math.max(prio1, prio2) match {
        case 0 => ByteArrayType
        case 1 => IntType
        case 2 => LongType
        case 3 => FloatType
        case 4 => DoubleType
        case _ => if (prio1 == 0) t2 else t1
      }
    }
  }

  def typeCompatibility(t1: PigType, t2: PigType): Boolean = {
    if (t1 == t2)
      true
    else if (t1.tc == TypeCode.AnyType || t2.tc == TypeCode.AnyType)
      true
    // numeric types are compatible
    else if (isNumericType(t1) && isNumericType(t2))
      true
    // simple type and complex type are not compatible
    else if (t1.isInstanceOf[SimpleType] && !t2.isInstanceOf[SimpleType] ||
      !t1.isInstanceOf[SimpleType] && t2.isInstanceOf[SimpleType])
      false
    // bytearray and numeric are compatible
    else if (isNumericType(t1) && t2 == Types.ByteArrayType || t1 == Types.ByteArrayType && isNumericType(t2))
      true
    // numeric and non-numeric types are not compatible
    else if (isNumericType(t1) && !isNumericType(t2) || !isNumericType(t1) && isNumericType(t2))
      false
    // chararray and bytearray are compatible
    else if (t1 == CharArrayType && t2 == ByteArrayType || t1 == ByteArrayType && t2 == CharArrayType)
      true
    else t1 match {
      case bag1: BagType if t2.isInstanceOf[BagType] =>
        val bag2 = t2.asInstanceOf[BagType]
        typeCompatibility(bag1.valueType, bag2.valueType)

      case _ => t1 match {
        case tuple1: TupleType if t2.isInstanceOf[TupleType] =>
          val tuple2 = t2.asInstanceOf[TupleType]
          if (tuple1.fields.length == tuple2.fields.length) {
            val fieldPairs = tuple1.fields.zip(tuple2.fields)
            !fieldPairs.exists { case (f1: Field, f2: Field) => !typeCompatibility(f1.fType, f2.fType) }
          }
          else
            false
        case _ => false
      }
    }
  }

  /**
   * Returns true if the given type is a numeric type.
   *
   * @param t the type to be checked
   * @return true if int, long, float or double
   */
  def isNumericType(t: PigType): Boolean = t.tc == TypeCode.IntType ||
    t.tc == TypeCode.LongType || t.tc == TypeCode.FloatType || t.tc == TypeCode.DoubleType

  /**
   * Predefined type instances for simple types.
   */
  val AnyType = SimpleType("nothing", TypeCode.AnyType)
  val IntType = SimpleType("int", TypeCode.IntType)
  val LongType = SimpleType("long", TypeCode.LongType)
  val BooleanType = SimpleType("boolean", TypeCode.BooleanType)
  val FloatType = SimpleType("float", TypeCode.FloatType)
  val DoubleType = SimpleType("double", TypeCode.DoubleType)
  val ByteArrayType = SimpleType("bytearray", TypeCode.ByteArrayType)
  val CharArrayType = SimpleType("chararray", TypeCode.CharArrayType)
  val stObjectType = STObjectType()


  /**
   * Returns the type object representing the type of the given name.
   *
   * @param s the name of the type
   * @return the type instance
   */
  @throws[TypeException]("if the type name is unknown")
  def typeForName(s: String) = s match {
    case "int" => IntType
    case "long" => LongType
    case "boolean" => BooleanType
    case "float" => FloatType
    case "double" => DoubleType
    case "bytearray" => ByteArrayType
    case "chararray" => CharArrayType
    case _ => throw TypeException("invalid type: " + s)
  }
}