package dbis.test.pig

import dbis.pig.schema._
import org.scalatest.FlatSpec
import dbis.pig._

/**
 * Created by kai on 09.04.15.
 */
class SchemaSpec extends FlatSpec {

  "The schema" should "contain f1, f2" in {
    val schema = new Schema(BagType("s", TupleType("t", Array(Field("f1", Types.DoubleType),
                                                              Field("f2", Types.CharArrayType)))))
    assert(schema.indexOfField("f1") == 0)
    assert(schema.indexOfField("f2") == 1)
    assert(schema.field(0) == Field("f1", Types.DoubleType))
    assert(schema.field(1) == Field("f2", Types.CharArrayType))
  }
}
