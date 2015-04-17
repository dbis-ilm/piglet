package dbis.pig

/**
 * Created by kai on 16.04.15.
 */

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
  val IntType, LongType, FloatType, BooleanType, DoubleType, ByteArrayType, CharArrayType = Value
}

import TypeCode._

sealed abstract class PigType(val name: String)

case class SimpleType(override val name: String, tc: TypeCode) extends PigType(name)

object Types {
  def typeCompatibility(t1: PigType, t2: PigType): Boolean = {
    // numeric types are compatible
    if (isNumericType(t1) && isNumericType(t2))
      true
    // simple type and complex type are not compatible
    else if (t1.isInstanceOf[SimpleType] && !t2.isInstanceOf[SimpleType] ||
      !t1.isInstanceOf[SimpleType] && t2.isInstanceOf[SimpleType])
      false
    // numeric and non-numeric types are not compatible
    else if (isNumericType(t1) && !isNumericType(t2) || !isNumericType(t1) && isNumericType(t2))
      false
    // chararray and bytearray are compatible
    else if (t1 == CharArrayType && t2 == ByteArrayType || t1 == ByteArrayType && t2 == CharArrayType)
      true
    else
      false
  }

  def isNumericType(t: PigType): Boolean = t == IntType || t == LongType || t == FloatType || t == DoubleType

  val IntType = SimpleType("int", TypeCode.IntType)
  val LongType = SimpleType("long", TypeCode.LongType)
  val BooleanType = SimpleType("boolean", TypeCode.BooleanType)
  val FloatType = SimpleType("float", TypeCode.FloatType)
  val DoubleType = SimpleType("double", TypeCode.DoubleType)
  val ByteArrayType = SimpleType("bytearray", TypeCode.ByteArrayType)
  val CharArrayType = SimpleType("chararray", TypeCode.CharArrayType)

  def typeForName(s: String) = s match {
    case "int" => IntType
    case "long" => LongType
    case "boolean" => BooleanType
    case "float" => FloatType
    case "double" => DoubleType
    case "bytearray" => ByteArrayType
    case "chararray" => CharArrayType
    case _ => throw new TypeException("invalid type: " + s)
  }
}

case class Field(name: String, fType: PigType)
case class TupleType(override val name: String, var fields: Array[Field]) extends PigType(name) {
  override def equals(that: Any): Boolean = that match {
    case TupleType(name, fields) => this.name == name && this.fields.deep == fields.deep
    case _ => false
  }
}

case class BagType(override val name: String, var valueType: PigType) extends PigType(name)

case class MapType(override val name: String, var valueType: PigType) extends PigType(name)