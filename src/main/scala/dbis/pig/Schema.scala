package dbis.pig

/**
 * Created by kai on 08.04.15.
 */
object FieldType extends Enumeration {
  type FieldType = Value
  val IntType, DoubleType, StringType = Value
}

import FieldType._

case class FieldDef(val name: String, val ftype: FieldType)

case class Schema(val fields: Vector[FieldDef])
