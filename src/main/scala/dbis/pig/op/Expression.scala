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

/**
 * A trait for all types of expressions.
 */
trait Expr {
  /**
   * Traverses the expression tree and applies the traverser function to each node.
   * The final boolean result is constructed by ANDing the result of the traverser
   * and the results from applying the traverser to all sub-nodes in the tree.
   * 
   * @param schema
   * @param traverser
   * @return
   */
  def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean

  /**
   * Traverses the expression tree and applies the traverser function to each node.
   * The final boolean result is constructed by ORing the result of the traverser
   * and the results from applying the traverser to all sub-nodes in the tree.
   *
   * @param schema
   * @param traverser
   * @return
   */
  def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean

  /**
   * Determines the result type of the expression.
   * 
   * @param schema The input schema for the operator providing the context of this expression (if defined).
   * @return
   */
  def resultType(schema: Option[Schema]): (String, PigType)
}

abstract class BinaryExpr(left: Expr, right: Expr) extends Expr {
  override def traverseAnd(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) && left.traverseAnd(schema, traverser) && right.traverseAnd(schema, traverser)


  override def traverseOr(schema: Schema, traverser: (Schema, Expr) => Boolean): Boolean =
    traverser(schema, this) || left.traverseOr(schema, traverser) || right.traverseOr(schema, traverser)

}

object Expr {
  /**
   * This function is a traverser function used as parameter to traverse.
   * It checks the (named) fields referenced in the given expression for conformance to
   * the schema.
   *
   * @param schema the schema of the operator
   * @param ex the expression containing fields
   * @return true if all named fields were found, false otherwise
   */
  def checkExpressionConformance(schema: Schema, ex: Expr): Boolean = ex match {
    case RefExpr(r) => r match {
      case NamedField(n) => schema.indexOfField(n) != -1 // TODO: we should produce an error message
      case _ => true
    }
    case _ => true
  }

  /**
   * This function is a traverser function used as parameter to traverse.
   * It checks whether the expression contains any named field.
   *
   * @param schema the schema of the operator
   * @param ex the expression containing fields
   * @return true if all the expression doesn't contain any named field
   */
  def containsNoNamedFields(schema: Schema, ex: Expr): Boolean = ex match {
    case RefExpr(r) => r match {
      case NamedField(n) => false
      case _ => true
    }
    case _ => true
  }

  /**
   * This function is a traverser function to check whether the expression contains
   * a flatten operator.
   *
   * @param schema the schema of the operator
   * @param ex the expression
   * @return true if the expression contains flatten.
   */
  def containsFlatten(schema: Schema, ex: Expr): Boolean = ex match {
    case FlattenExpr(e) => true
    case _ => false
  }


  /**
   * This function is a traverser function to check whether the expression contains
   * a flatten operator for a bag.
   *
   * @param schema the schema of the operator
   * @param ex the expression
   * @return true if the expression contains flatten({}).
   */
  def containsFlattenOnBag(schema: Schema, ex: Expr): Boolean = {
    ex match {
      case FlattenExpr(e) => e.resultType(Some(schema))._2.isInstanceOf[BagType]
      case _ => false
    }
  }
}