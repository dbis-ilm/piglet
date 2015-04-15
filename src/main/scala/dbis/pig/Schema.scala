package dbis.pig

/**
 * Created by kai on 08.04.15.
 */
/**
 * An enumeration of possible field types.
 */
object FieldType extends Enumeration {
  type FieldType = Value
  val IntType, LongType, DoubleType, CharArrayType, ByteArrayType, BagType = Value
}

import FieldType._

/**
 * An exception indicating failures in schema handling.
 *
 * @param msg a message describing the exeption.
 */
case class SchemaException(msg: String) extends Exception(msg)

/**
 * A field definition describes the structure of a field (column).
 *
 * @param name the name of the field
 * @param ftype the type of the field
 */
case class FieldDef(val name: String, val ftype: FieldType)

/**
 * A schema describes the structure of the output relation of an operator.
 *
 * @param fields the list of fields (columns) consisting of name and type.
 */
case class Schema(val fields: Vector[FieldDef]) {
  /**
   * Returns the index of the field in the schema.
   *
   * @param name the name of the field
   * @return the position in the field list
   */
  def indexOfField(name: String) : Int = {
    fields.indexWhere(_.name == name)
  }

  /**
   * Returns the field at the given position.
   *
   * @param pos the position of the field in the schemas field list
   * @return the field definition
   */
  def field(pos: Int): FieldDef = {
    fields(pos)
  }
}
