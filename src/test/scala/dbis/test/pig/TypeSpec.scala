package dbis.test.pig

import dbis.pig._
import org.scalatest.FlatSpec

/**
 * Created by kai on 16.04.15.
 */
class TypeSpec extends FlatSpec {

  "The type system" should "contain predefined simple types" in {
    val t1 = Types.IntType
    assert(t1.name == "int")
    assert(t1 == Types.typeForName("int"))

    val t2 = Types.FloatType
    assert(t2.name == "float")
    assert(t2 == Types.typeForName("float"))

    val t3 = Types.DoubleType
    assert(t3.name == "double")
    assert(t3 == Types.typeForName("double"))

    val t4 = Types.CharArrayType
    assert(t4.name == "chararray")
    assert(t4 == Types.typeForName("chararray"))

    val t5 = Types.ByteArrayType
    assert(t5.name == "bytearray")
    assert(t5 == Types.typeForName("bytearray"))

    val t6 = Types.LongType
    assert(t6.name == "long")
    assert(t6 == Types.typeForName("long"))
  }

  it should "allow to define complex types" in {
    val t1 = MapType("m", Types.DoubleType)
    assert(t1.name == "m")
    assert(t1.valueType == Types.typeForName("double"))

    val t2 = BagType("b", Types.CharArrayType)
    assert(t2.name == "b")
    assert(t2.valueType == Types.CharArrayType)

    val t3 = TupleType("t", Array(Field("f1", Types.IntType),
                                  Field("f2", Types.DoubleType),
                                  Field("f3", MapType("m", Types.CharArrayType))))
    assert(t3.name == "t")
    assert(t3.fields(0).name == "f1")
    assert(t3.fields(1).name == "f2")
    assert(t3.fields(1).fType == Types.DoubleType)
    assert(t3.fields(2).name == "f3")
    assert(t3.fields(2).fType.name == "m")
  }

  it should "check type compatibility" in {
    assert(Types.typeCompatibility(Types.IntType, Types.LongType))
    assert(Types.typeCompatibility(Types.IntType, Types.DoubleType))
    assert(Types.typeCompatibility(Types.FloatType, Types.DoubleType))
    assert(! Types.typeCompatibility(Types.IntType, Types.CharArrayType))
    assert(! Types.typeCompatibility(Types.IntType, MapType("m", Types.CharArrayType)))
  }
}