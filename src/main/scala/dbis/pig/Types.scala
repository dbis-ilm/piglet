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

/**
 * The base class for all Pig types.
 */
sealed abstract class PigType {
  var name: String = ""

  def this(s: String) = { this(); name = s }

  def descriptionString: String = name
}

/**
 * The base class for all primitive types (int, float, double, ...).
 *
 * @param s the name of the type.
 * @param tc the typecode representing this type.
 */
case class SimpleType(s: String, tc: TypeCode) extends PigType(s)

object Types {
  def typePriority(t: PigType): Int = t match {
    // double > float > long > int > bytearray
    case ByteArrayType => 0
    case IntType => 1
    case LongType => 2
    case FloatType => 3
    case DoubleType => 4
    // tuple|bag|map|chararray > bytearray
    case CharArrayType => 10
    case _ => 10
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
    else if (t1.isInstanceOf[BagType] && t2.isInstanceOf[BagType]) {
      // two bags are compatible if their value types are compatible
      val bag1 = t1.asInstanceOf[BagType]
      val bag2 = t2.asInstanceOf[BagType]
      typeCompatibility(bag1.valueType, bag2.valueType)
    }
    else if (t1.isInstanceOf[TupleType] && t2.isInstanceOf[TupleType]) {
      // two tuples are compatible if they have the same number of fields + compatible fields
      val tuple1 = t1.asInstanceOf[TupleType]
      val tuple2 = t2.asInstanceOf[TupleType]
      if (tuple1.fields.length == tuple2.fields.length) {
        val fieldPairs = tuple1.fields.zip(tuple2.fields)
        !fieldPairs.exists{case (f1: Field, f2: Field) => ! typeCompatibility(f1.fType, f2.fType)}
      }
      else
        false
    }
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

case class Field(name: String, fType: PigType = Types.ByteArrayType) {
  override def toString = s"${name}: ${fType.descriptionString}"
}

case class TupleType(s: String, var fields: Array[Field]) extends PigType(s) {
  override def equals(that: Any): Boolean = that match {
    case TupleType(name, fields) => this.name == name && this.fields.deep == fields.deep
    case _ => false
  }

  override def toString = "TupleType(" + name + "," + fields.mkString(",") + ")"

  override def descriptionString = "(" + fields.mkString(", ") + ")"

  def plainDescriptionString = fields.mkString(", ")
}

case class BagType(s: String, var valueType: TupleType) extends PigType(s) {
  override def descriptionString = "{" + valueType.plainDescriptionString + "}"
}

case class MapType(s: String, var valueType: PigType) extends PigType(s) {
  override def descriptionString = "[" + valueType.descriptionString + "]"
}