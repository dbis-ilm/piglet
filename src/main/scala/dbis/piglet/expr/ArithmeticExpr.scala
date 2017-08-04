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

package dbis.piglet.expr

import dbis.piglet.schema._
import dbis.piglet.udf.UDFTable

import scala.collection.mutable

trait ArithmeticExpr extends Expr

case class RefExpr(var r: Ref) extends ArithmeticExpr {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = traverser(schema, this)
  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = traverser(schema, this)

  override def resultType(schema: Option[Schema]): PigType = schema match {
    case Some(s) => r match {
      case nf : NamedField => try {
        val f = s.field(nf)
        f.fType
      }
      catch {
        case _: SchemaException =>
//          logger.warn("cannot find result type",e)
          Types.AnyType
      }
      case PositionalField(p) => val f = s.field(p); f.fType
      case Value(v) => v match {
        case _: String => Types.CharArrayType
        case _ => v match {
          case _: Int => Types.IntType
          case _: Double => Types.DoubleType
          case _ => Types.ByteArrayType
        }
      }
      case DerefTuple(t, c) =>
        // in case of tuple.NamedField or tuple.PositionalField, we need
        // to detemine the field type
        val bagField = t match {
          case NamedField(n, _) => s.field(n)
          case PositionalField(p) =>  s.field(p)
          case _ => throw SchemaException(s"unknown bag in the schema")
        }
        val tupleType = bagField.fType match {
          case tupleType1: TupleType => tupleType1
          case _ => bagField.fType.asInstanceOf[BagType].valueType
        }
        val fieldType: PigType = c match {
          case NamedField(n, _) => tupleType.fields.filter { p => p.name == n}.head.fType
          case PositionalField(p) => tupleType.fields(p).fType
          case _ => null
        }
        if (fieldType != null) fieldType else Types.ByteArrayType
      //case DerefMap(m, k) =>
      case _ => Types.ByteArrayType
    }
    case None => Types.ByteArrayType
  }

  override def exprName(): String = r match {
    case NamedField(n, _) =>  n
    case _  => ""
  }

  override def toString = r.toString

  def resolveReferences(mapping: mutable.Map[String, Ref]): Unit = r match {
    case NamedField(n, _) =>
      val newVal = replaceReference(n, mapping)
      if (newVal != n) {
        /*
         * If we have replaced the name of a NamedField it means that we
         * don't have a field reference anymore, but a value. So, let's
         * replace it.
         */
        r = Value(newVal)
      }
    case v@Value(n) => n match {
      case str: String => v.v = replaceReference(str, mapping)
      case _ =>
    }
    case _ =>
  }

  def replaceReference(s: String, mapping: mutable.Map[String, Ref]): Any = {
    if (s.startsWith("$") && mapping.contains(s)) {
      val s2 = mapping(s) match {
        case NamedField(n, _) => n
        case Value(v) => v
        case _ => s
      }
      s2
    }
    else s
  }
}

case class FlattenExpr(a: ArithmeticExpr) extends ArithmeticExpr {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) && a.traverseAnd(schema, traverser)
  }

  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) || a.traverseOr(schema, traverser)
  }

  override def resultType(schema: Option[Schema]): PigType = {
    val bType = a.resultType(schema) // that's a BagType, extract the component type
    bType match {
      case cType: ComplexType =>
        cType.typeOfComponent(0)
      case _ =>
        a.resultType(schema)
    }
  }

  override def resolveReferences(mapping: mutable.Map[String, Ref]): Unit = a.resolveReferences(mapping)
}

case class PExpr(a: ArithmeticExpr) extends ArithmeticExpr {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) && a.traverseAnd(schema, traverser)
  }

  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) || a.traverseOr(schema, traverser)
  }

  override def resultType(schema: Option[Schema]): PigType = a.resultType(schema)

  override def resolveReferences(mapping: mutable.Map[String, Ref]): Unit = a.resolveReferences(mapping)
}

case class CastExpr(t: PigType, a: ArithmeticExpr) extends ArithmeticExpr {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) && a.traverseAnd(schema, traverser)
  }

  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) || a.traverseOr(schema, traverser)
  }

  override def resultType(schema: Option[Schema]): PigType = t

  override def resolveReferences(mapping: mutable.Map[String, Ref]): Unit = a.resolveReferences(mapping)
}

case class MSign(a: ArithmeticExpr) extends ArithmeticExpr {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) && a.traverseAnd(schema, traverser)
  }
  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) || a.traverseOr(schema, traverser)
  }
  override def resultType(schema: Option[Schema]): PigType = a.resultType(schema)

  override def resolveReferences(mapping: mutable.Map[String, Ref]): Unit = a.resolveReferences(mapping)
}

case class Add(override val left: ArithmeticExpr,  override val right: ArithmeticExpr) extends BinaryExpr(left, right) with ArithmeticExpr {
  override def resultType(schema: Option[Schema]): PigType = {
    val res = if(left.resultType(schema) == Types.CharArrayType || right.resultType(schema) == Types.CharArrayType) Types.CharArrayType else Types.escalateTypes(left.resultType(schema), right.resultType(schema))
    if (res == Types.ByteArrayType) Types.DoubleType else res
  }
}

case class Minus(override val left: ArithmeticExpr,  override val right: ArithmeticExpr) extends BinaryExpr(left, right) with ArithmeticExpr {
  override def resultType(schema: Option[Schema]): PigType = {
    val res = Types.escalateTypes(left.resultType(schema), right.resultType(schema))
    if (res == Types.ByteArrayType) Types.DoubleType else res
  }}

case class Mult(override val left: ArithmeticExpr,  override val right: ArithmeticExpr) extends BinaryExpr(left, right) with ArithmeticExpr {
  override def resultType(schema: Option[Schema]): PigType = {
    val res = Types.escalateTypes(left.resultType(schema), right.resultType(schema))
    if (res == Types.ByteArrayType) Types.DoubleType else res
  }
}

case class Div(override val left: ArithmeticExpr,  override val right: ArithmeticExpr) extends BinaryExpr(left, right) with ArithmeticExpr {
  override def resultType(schema: Option[Schema]): PigType = {
    val res = Types.escalateTypes(left.resultType(schema), right.resultType(schema))
    if (res == Types.ByteArrayType) Types.DoubleType else res
  }
}

case class Func(f: String, params: List[ArithmeticExpr]) extends ArithmeticExpr {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) &&
      params.map(_.traverseAnd(schema, traverser)).forall(b => b)
  }

  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) ||
    params.map(_.traverseOr(schema, traverser)).exists(b => b)
  }


  override def resultType(schema: Option[Schema]): PigType = {
    val pTypes = params.map(e => e.resultType(schema))
    val func = UDFTable.findUDF(f, pTypes)
    func match {
      case Some(udf) => udf.resultType
      case None => Types.ByteArrayType
    }
  }

  override def resolveReferences(mapping: mutable.Map[String, Ref]): Unit = params.foreach(_.resolveReferences(mapping))
}

trait ConstructExpr extends ArithmeticExpr {
  var exprs: List[ArithmeticExpr] = _

  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) &&
      exprs.map(_.traverseAnd(schema, traverser)).forall(b => b)
  }

  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) ||
      exprs.map(_.traverseOr(schema, traverser)).exists(b => b)
  }

  override def resolveReferences(mapping: mutable.Map[String, Ref]): Unit = exprs.foreach(_.resolveReferences(mapping))

  def exprListToTuple(schema: Option[Schema]): Array[Field] =
    exprs.map(e => e.resultType(schema)).zipWithIndex.map(f => Field("", f._1)).toArray
    //exprs.map(e => e.resultType(schema)).zipWithIndex.map(f => Field(s"f${f._2}", f._1)).toArray
}

case class ConstructTupleExpr(ex: List[ArithmeticExpr]) extends ConstructExpr {
  exprs = ex

  override def resultType(schema: Option[Schema]): PigType = TupleType(exprListToTuple(schema))
}

/**
 * An expression for constructing a bag from a list of expressions.
 *
 * @param ex the expression list given as argument to the bag constructor
 */
case class ConstructBagExpr(ex: List[ArithmeticExpr]) extends ConstructExpr {
  exprs = ex

  /**
   * Returns the type of the expression: we take the most general type of the argument list
   * and construct a bag from it.
   *
   * @param schema The input schema for the operator providing the context of this expression (if defined).
   * @return the result type
   */
  override def resultType(schema: Option[Schema]): PigType = {
    val argTypes = exprs.map(e => e.resultType(schema))
    BagType(TupleType(Array(Field("", argTypes.head))))
  }
}

case class ConstructMapExpr(ex: List[ArithmeticExpr]) extends ConstructExpr {
  exprs = ex

  /**
   * Returns the type of the expression: assuming an expression [key, value] we simply take
   * the  type of the value expression as the result type.
   *
   * @param schema The input schema for the operator providing the context of this expression (if defined).
   * @return the result type
   */
  override def resultType(schema: Option[Schema]): PigType = {
    MapType(exprs(1).resultType(schema))
  }
}

case class ConstructMatrixExpr(typeString: String, rows: Int, cols: Int, ex: ArithmeticExpr) extends ConstructExpr {
  exprs = List(ex)

  require (typeString.matches("[sd][di]"))

  override def resultType(schema: Option[Schema]): PigType = {
    val t = if (typeString.charAt(1) == 'i') Types.IntType else Types.DoubleType
    val k = if (typeString.charAt(0) == 's') MatrixRep.SparseMatrix else MatrixRep.DenseMatrix
    if (ex.resultType(schema).tc != TypeCode.BagType )
      throw SchemaException(s"matrix construction requires a bag parameter")
    MatrixType(t, rows, cols, k)
  }
}

trait TempEx
case class Instant(value: ArithmeticExpr) extends TempEx
case class Interval(start: ArithmeticExpr, stop: Option[ArithmeticExpr]) extends TempEx

case class ConstructGeometryExpr(ex: ArithmeticExpr, time: Option[TempEx]) extends ConstructExpr {
  exprs = List(ex)
  
  override def resultType(schema: Option[Schema]): PigType = {
    if(ex.resultType(schema).tc != TypeCode.CharArrayType)
      throw SchemaException(s"geometry construction requires a string parameter, but is ${ex.resultType(schema).tc}")
    
    GeometryType()
  }
  
}

