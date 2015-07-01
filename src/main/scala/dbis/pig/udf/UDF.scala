package dbis.pig.udf

import dbis.pig.schema._

/**
 * Created by kai on 26.06.15.
 */
case class UDF(name: String, scalaName: String, paramTypes: List[PigType], resultType: PigType, isAggregate: Boolean) {
  def numParams = paramTypes.size
}

object UDFTable {
  lazy val funcTable: List[UDF] = List(
    UDF("COUNT", "PigFuncs.count", List(Types.AnyType), Types.IntType, true),
    UDF("AVG", "PigFuncs.average", List(Types.IntType), Types.DoubleType, true),
    UDF("AVG", "PigFuncs.average", List(Types.LongType), Types.DoubleType, true),
    UDF("AVG", "PigFuncs.average", List(Types.FloatType), Types.DoubleType, true),
    UDF("AVG", "PigFuncs.average", List(Types.DoubleType), Types.DoubleType, true),
    UDF("SUM", "PigFuncs.sum", List(Types.IntType), Types.IntType, true),
    UDF("SUM", "PigFuncs.sum", List(Types.LongType), Types.LongType, true),
    UDF("SUM", "PigFuncs.sum", List(Types.FloatType), Types.FloatType, true),
    UDF("SUM", "PigFuncs.sum", List(Types.DoubleType), Types.DoubleType, true),
    UDF("MIN", "PigFuncs.min", List(Types.IntType), Types.IntType, true),
    UDF("MIN", "PigFuncs.min", List(Types.LongType), Types.LongType, true),
    UDF("MIN", "PigFuncs.min", List(Types.FloatType), Types.FloatType, true),
    UDF("MIN", "PigFuncs.min", List(Types.DoubleType), Types.DoubleType, true),
    UDF("MAX", "PigFuncs.max", List(Types.IntType), Types.IntType, true),
    UDF("MAX", "PigFuncs.max", List(Types.LongType), Types.LongType, true),
    UDF("MAX", "PigFuncs.max", List(Types.FloatType), Types.FloatType, true),
    UDF("MAX", "PigFuncs.max", List(Types.DoubleType), Types.DoubleType, true),
    UDF("TOKENIZE", "PigFuncs.tokenize", List(Types.CharArrayType), BagType(TupleType(Array(Field("", Types.ByteArrayType)))), false),
    UDF("TOMAP", "PigFuncs.toMap", List(Types.AnyType), MapType(Types.ByteArrayType), false)
  )

  def typeMatch(funcType: PigType, paramType: PigType): Boolean = if (funcType == Types.AnyType) true else funcType == paramType

  // TODO
  def typeListMatch(funcTypes: List[PigType], paramTypes: List[PigType]): Boolean = true

  // TODO
  def typeListCompatibility(funcTypes: List[PigType], paramTypes: List[PigType]): Boolean = true

  def findUDF(name: String, paramType: PigType): Option[UDF] = {
    // 1st, we check candidates with the same name
    val candidates = funcTable.filter(udf => udf.name == name.toUpperCase && udf.numParams == 1)
    // if we find a udf among these candidates with the same number and type of parameter, then we return it directly
    val res = candidates.filter{udf: UDF => typeMatch(udf.paramTypes.head, paramType)}
    if (res.nonEmpty)
      res.headOption
    else {
      // otherwise we check for a udf with type compatible parameter
      candidates.find { udf: UDF => Types.typeCompatibility(udf.paramTypes.head, paramType) }
    }
  }

  def findUDF(name: String, paramTypes: List[PigType]): Option[UDF] = {
    // 1st, we check candidates with the same name (not the same number of parameters in order to allow AnyType)
    val candidates = funcTable.filter(udf => udf.name == name.toUpperCase /* && udf.numParams == paramTypes.size*/)
    // if we find a udf among these candidates with the same number and type of parameters, then we return it directly
    val res = candidates.filter { udf: UDF => typeListMatch(udf.paramTypes, paramTypes) }
    if (res.nonEmpty)
      res.headOption
    else {
      // otherwise we check for a udf with type compatible parameter
      candidates.find { udf: UDF => typeListCompatibility(udf.paramTypes, paramTypes) }
    }
  }
}
