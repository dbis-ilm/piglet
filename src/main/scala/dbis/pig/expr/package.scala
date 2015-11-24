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
package dbis.pig.expr

/**
 * This package provides traits and classes for representing Pig expressions.
 *
 * ==Overview==
 * 
 * The base trait for all kind of expressions is [[dbis.pig.expr.Expr]] which defines the following methods
 *   * traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean
 *   * traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean
 *     These methods traverse the expression tree and applies the traverser function and return a boolean
 *     value. With traverseAnd the results for the different expressions nodes are ANDed, with
 *     traverseOr they are ORed. These methods are mainly used to.
 *
 *   * resultType(schema: Option[Schema]): PigType
 *     This method determines the result type of the expression by taking information from the schema
 *     into accout.
 *
 *   * exprName(): String
 *     ???
 *
 *   * resolveReferences(mapping: Map[String, Ref]): Unit
 *     Using this method all references in expressions with a leading $ are replaced by Refs which
 *     are given in the mapping table. This method is needed for dealing with macros.
 *
 * For logical predicates the [[dbis.pig.expr.Predicate]] is used which is derived from [[dbis.pig.expr.Expr]]
 * but does not add further methods.
 *
 * Arithmetic expressions are represented by classes which implement the [[dbis.pig.expr.ArithmeticExpr]] trait
 * which is also derived from [[dbis.pig.expr.Expr]].
 *
 * Concrete expression classes for predicates are:
 *    * And: expr1 AND expr2
 *    * Or: expr1 OR expr2
 *    * Not: NOT expr
 *    * Eq: expr1 == expr2
 *    * Neq: expr1 != expr2
 *    * Geq: expr1 >= expr2
 *    * Gt: expr1 > expr2
 *    * Leq: expr1 <= expr2
 *    * Lt: expr1 < expr2
 *    * PPredicate: ( expr )
 *
 *  Arithmetic expression classes are:
 *    * Add: expr1 + expr2
 *    * Minus: expr1 - expr2
 *    * Mult: expr1 * expr2
 *    * Div: expr1 / expr2
 *    * MSign: -expr
 *    * PExpr: ( expr )
 *    * FlattenExpr: flatten(expr)
 *    * CastExpr: (type) expr
 *    * FuncExpr: func(expr1, ..., exprN)
 *    * ConstructBagExpr: { expr1, expr2, ..., exprN } -> bag
 *    * ConstructMapExpr: [ key1#value1, key2#value2 ] -> map
 *    * ConstructTupleExpr: ( expr1, expr2, ..., exprN ) -> tuple
 *    * RefExpr:
 *
 */

package expr {}