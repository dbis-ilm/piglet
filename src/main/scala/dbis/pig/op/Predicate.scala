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

import dbis.pig.schema.{Types, PigType, Schema}

import scala.collection.mutable.Map

trait Predicate extends Expr

case class Eq(override val left: ArithmeticExpr, override val right: ArithmeticExpr) extends BinaryExpr(left, right) with Predicate {
  
  override def resultType(schema: Option[Schema]): PigType = Types.BooleanType

  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) && left.traverseAnd(schema, traverser) && right.traverseAnd(schema, traverser)


  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) || left.traverseAnd(schema, traverser) || right.traverseAnd(schema, traverser)
}

case class Neq(override val left: ArithmeticExpr, override val right: ArithmeticExpr) extends BinaryExpr(left, right) with Predicate {
  override def resultType(schema: Option[Schema]): PigType = Types.BooleanType

  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) && left.traverseAnd(schema, traverser) && right.traverseAnd(schema, traverser)


  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) || left.traverseAnd(schema, traverser) || right.traverseAnd(schema, traverser)
}

case class Geq(override val left: ArithmeticExpr,  override val right: ArithmeticExpr) extends BinaryExpr(left, right) with Predicate {
  override def resultType(schema: Option[Schema]): PigType = Types.BooleanType

  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) && left.traverseAnd(schema, traverser) && right.traverseAnd(schema, traverser)


  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) || left.traverseAnd(schema, traverser) || right.traverseAnd(schema, traverser)
}

case class Leq(override val left: ArithmeticExpr,  override val right: ArithmeticExpr) extends BinaryExpr(left, right) with Predicate {
  override def resultType(schema: Option[Schema]): PigType = Types.BooleanType

  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) && left.traverseAnd(schema, traverser) && right.traverseAnd(schema, traverser)


  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) || left.traverseAnd(schema, traverser) || right.traverseAnd(schema, traverser)
}

case class Gt(override val left: ArithmeticExpr,  override val right: ArithmeticExpr) extends BinaryExpr(left, right) with Predicate {
  override def resultType(schema: Option[Schema]): PigType = Types.BooleanType

  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) && left.traverseAnd(schema, traverser) && right.traverseAnd(schema, traverser)


  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) || left.traverseAnd(schema, traverser) || right.traverseAnd(schema, traverser)
}

case class Lt(override val left: ArithmeticExpr,  override val right: ArithmeticExpr) extends BinaryExpr(left, right) with Predicate {
  override def resultType(schema: Option[Schema]): PigType = Types.BooleanType

  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) && left.traverseAnd(schema, traverser) && right.traverseAnd(schema, traverser)


  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) || left.traverseAnd(schema, traverser) || right.traverseAnd(schema, traverser)
}

case class And(a: Predicate, b: Predicate) extends Predicate {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) && a.traverseAnd(schema, traverser) && b.traverseAnd(schema, traverser)


  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) || a.traverseAnd(schema, traverser) || b.traverseAnd(schema, traverser)

  override def resultType(schema: Option[Schema]): PigType = Types.BooleanType

  override def resolveReferences(mapping: Map[String, Ref]): Unit = {
    a.resolveReferences(mapping)
    b.resolveReferences(mapping)
  }
}

case class Or(a: Predicate, b: Predicate) extends Predicate {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) && a.traverseAnd(schema, traverser) && b.traverseAnd(schema, traverser)


  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) || a.traverseAnd(schema, traverser) || b.traverseAnd(schema, traverser)

  override def resultType(schema: Option[Schema]): PigType = Types.BooleanType

  override def resolveReferences(mapping: Map[String, Ref]): Unit = {
    a.resolveReferences(mapping)
    b.resolveReferences(mapping)
  }
}

case class Not(a: Predicate) extends Predicate {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) && a.traverseAnd(schema, traverser)


  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) || a.traverseAnd(schema, traverser)

  override def resultType(schema: Option[Schema]): PigType = Types.BooleanType

  override def resolveReferences(mapping: Map[String, Ref]): Unit = a.resolveReferences(mapping)
}

/**
 * A parenthesized predicate.
 *
 * @param a
 */
case class PPredicate(a: Predicate) extends Predicate {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) && a.traverseAnd(schema, traverser)


  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) || a.traverseAnd(schema, traverser)

  override def resultType(schema: Option[Schema]): PigType = Types.BooleanType

  override def resolveReferences(mapping: Map[String, Ref]): Unit = a.resolveReferences(mapping)
}