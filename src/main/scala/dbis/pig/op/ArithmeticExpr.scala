/*
 * Copyright (c) 2015 The Piglet team,
 *                    All Rights Reserved.
 *
 * This file is part of the Piglet package.
 *
 * PipeFabric is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License (GPL) as
 * published by the Free Software Foundation; either version 2 of
 * the License, or (at your option) any later version.
 *
 * This package is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; see the file LICENSE.
 * If not you can find the GPL at http://www.gnu.org/copyleft/gpl.html
 */
package dbis.pig.op

import dbis.pig.schema.{Types, PigType, Schema}

trait ArithmeticExpr extends Expr

case class RefExpr(r: Ref) extends ArithmeticExpr {
  override def traverse(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this)
  }

  override def resultType(schema: Option[Schema]): (String, PigType) = schema match {
    case Some(s) => r match {
      case NamedField(n) => val f = s.field(n); (n, f.fType)
      case PositionalField(p) => val f = s.field(p); ("", Types.ByteArrayType)
      case Value(v) => if (v.isInstanceOf[String]) ("", Types.CharArrayType) else ("", Types.ByteArrayType)
      // TODO: handle deref of tuple, bag
      //case DerefTuple(t, c) =>
      //case DerefMap(m, k) =>
    }
    case None => ("", Types.ByteArrayType)
  }
}

case class CastExpr(t: PigType, a: ArithmeticExpr) extends ArithmeticExpr {
  override def traverse(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) && a.traverse(schema, traverser)
  }

  override def resultType(schema: Option[Schema]): (String, PigType) = (a.resultType(schema)._1, t)
}

case class MSign(a: ArithmeticExpr) extends ArithmeticExpr {
  override def traverse(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) && a.traverse(schema, traverser)
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
  override def traverse(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean = {
    traverser(schema, this) &&
      params.map(_.traverse(schema, traverser)).foldLeft(true){ (b1: Boolean, b2: Boolean) => b1 && b2 }
  }

  // TODO: we should know the function signature
  override def resultType(schema: Option[Schema]): (String, PigType) = ("", Types.ByteArrayType)
}

