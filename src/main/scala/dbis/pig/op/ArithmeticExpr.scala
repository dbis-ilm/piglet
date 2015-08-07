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

package dbis.pig.op

import dbis.pig.schema._
import dbis.pig.udf.{UDFTable, UDF}

trait ArithmeticExpr extends Expr

case class RefExpr(r: Ref) extends ArithmeticExpr {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = traverser(schema, this)
  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = traverser(schema, this)

  override def resultType(schema: Option[Schema]): (String, PigType) = schema match {
    case Some(s) => r match {
      case NamedField(n) => try {
        val f = s.field(n)
        (n, f.fType)
      }
      catch {
        case e: SchemaException => (n, Types.AnyType)
      }
      case PositionalField(p) => val f = s.field(p); ("", f.fType)
      case Value(v) => if (v.isInstanceOf[String]) ("", Types.CharArrayType) else ("", Types.ByteArrayType)
      // TODO: handle deref of tuple, bag
      //case DerefTuple(t, c) =>
      //case DerefMap(m, k) =>
      case _ => ("", Types.ByteArrayType)
    }
    case None => ("", Types.ByteArrayType)
  }
}

case class FlattenExpr(a: ArithmeticExpr) extends ArithmeticExpr {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) && a.traverseAnd(schema, traverser)
  }

  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) || a.traverseOr(schema, traverser)
  }

  override def resultType(schema: Option[Schema]): (String, PigType) = {
    val bType = a.resultType(schema) // that's a BagType, extract the component type
    if (bType._2.isInstanceOf[ComplexType]) {
      val cType = bType._2.asInstanceOf[ComplexType]
      ("", cType.typeOfComponent(0))
    }
    else {
      a.resultType(schema)
    }
  }
}

case class PExpr(a: ArithmeticExpr) extends ArithmeticExpr {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) && a.traverseAnd(schema, traverser)
  }

  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) || a.traverseOr(schema, traverser)
  }

  override def resultType(schema: Option[Schema]): (String, PigType) = a.resultType(schema)
}

case class CastExpr(t: PigType, a: ArithmeticExpr) extends ArithmeticExpr {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) && a.traverseAnd(schema, traverser)
  }

  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) || a.traverseOr(schema, traverser)
  }

  override def resultType(schema: Option[Schema]): (String, PigType) = (a.resultType(schema)._1, t)
}

case class MSign(a: ArithmeticExpr) extends ArithmeticExpr {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) && a.traverseAnd(schema, traverser)
  }
  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) || a.traverseOr(schema, traverser)
  }
  override def resultType(schema: Option[Schema]): (String, PigType) = a.resultType(schema)
}

case class Add(left: ArithmeticExpr, right: ArithmeticExpr) extends BinaryExpr(left, right) with ArithmeticExpr {
  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.DoubleType)
}

case class Minus(left: ArithmeticExpr, right: ArithmeticExpr) extends BinaryExpr(left, right) with ArithmeticExpr {
  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.DoubleType)
}

case class Mult(left: ArithmeticExpr, right: ArithmeticExpr) extends BinaryExpr(left, right) with ArithmeticExpr {
  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.DoubleType)
}

case class Div(left: ArithmeticExpr, right: ArithmeticExpr) extends BinaryExpr(left, right) with ArithmeticExpr {
  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.DoubleType)
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


  override def resultType(schema: Option[Schema]): (String, PigType) = {
    val pTypes = params.map(e => e.resultType(schema)._2)
    val func = UDFTable.findUDF(f, pTypes)
    func match {
      case Some(udf) => ("", udf.resultType)
      case None => ("", Types.ByteArrayType)
    }
  }
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

  def exprListToTuple(schema: Option[Schema]): Array[Field] =
    exprs.map(e => e.resultType(schema)._2).zipWithIndex.map(f => Field(s"f${f._2}", f._1)).toArray
}

case class ConstructTupleExpr(ex: List[ArithmeticExpr]) extends ConstructExpr {
  exprs = ex

  override def resultType(schema: Option[Schema]): (String, PigType) = ("", TupleType(exprListToTuple(schema)))
}

case class ConstructBagExpr(ex: List[ArithmeticExpr]) extends ConstructExpr {
  exprs = ex

  override def resultType(schema: Option[Schema]): (String, PigType) = ("", BagType(TupleType(exprListToTuple(schema))))
}

case class ConstructMapExpr(ex: List[ArithmeticExpr]) extends ConstructExpr {
  exprs = ex

  override def resultType(schema: Option[Schema]): (String, PigType) = ("", MapType(exprs(0).resultType(schema)._2))
}
