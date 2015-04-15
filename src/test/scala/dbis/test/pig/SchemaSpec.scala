package dbis.test.pig

import org.scalatest.FlatSpec
import dbis.pig._

/**
 * Created by kai on 09.04.15.
 */
class SchemaSpec extends FlatSpec {

  "The schema" should "contain f1, f2" in {
    val schema = new Schema(Vector(FieldDef("f1", FieldType.DoubleType), FieldDef("f2", FieldType.CharArrayType)))
    assert(schema.indexOfField("f1") == 0)
    assert(schema.indexOfField("f2") == 1)
    assert(schema.field(0) == FieldDef("f1", FieldType.DoubleType))
    assert(schema.field(1) == FieldDef("f2", FieldType.CharArrayType))
  }
}
