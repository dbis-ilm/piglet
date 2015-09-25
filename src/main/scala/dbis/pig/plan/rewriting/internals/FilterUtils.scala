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
package dbis.pig.plan.rewriting.internals

import dbis.pig.op._

object FilterUtils {
  /** Extract the [[dbis.pig.op.Predicate]] from one. This also extracts the Predicate from a [[dbis.pig.op.PPredicate]]
    */
  def extractPredicate(p: Predicate) = {
    if (p.isInstanceOf[PPredicate]) {
      p.asInstanceOf[PPredicate].a
    } else {
      p
    }
  }

  /** Extract all the [[dbis.pig.op.Ref]] values used in a [[dbis.pig.op.Expr]].
    */
  def extractFields(p: Expr): List[Ref] =
    p match {
      case RefExpr(Value(_)) => Nil
      case RefExpr(r) => List(r)
      case BinaryExpr(l, r) => extractFields(l) ++ extractFields(r)
      case And(l, r) => extractFields(l) ++ extractFields(r)
      case Or(l, r) => extractFields(l) ++ extractFields(r)
      case Not(p) => extractFields(p)
      case FlattenExpr(e) => extractFields(e)
      case PExpr(e) => extractFields(e)
      case CastExpr(t, e) => extractFields(e)
      case MSign(e) => extractFields(e)
      case Func(f, p) => p.flatMap(extractFields)
      case ConstructBagExpr(ex) => ex.flatMap(extractFields)
      case ConstructMapExpr(ex) => ex.flatMap(extractFields)
      case ConstructTupleExpr(ex) => ex.flatMap(extractFields)
    }

  /** Find a [[dbis.pig.op.PigOperator]] whose schema contains all the fields of ``namedFields``.
    */
  def findInputForFields(inputs: Seq[PigOperator], namedFields: Seq[NamedField]): Option[PigOperator] = {
    var inputWithCorrectFields: Option[PigOperator] = None
    inputs foreach { inp =>
      namedFields foreach { f =>
        if (inp.schema.isEmpty) {
          // The schema of an input is not defined, abort because it might or might not contain one of the fields
          // we're looking for
          return None
        }

        if (inp.schema.get.fields.exists(_.name == f.name)) {
          if (inputWithCorrectFields.isEmpty) {
            // We found an input that has the correct field and we haven't found one with a matching field before
            inputWithCorrectFields = Some(inp)
          }

          if (inputWithCorrectFields.get != inp) {
            // We found an input that has a field we're looking for but we already found an input with a matching
            // field but it's not the same as the current input.
            // TODO Just abort the operation for now - in the future, we could try to split push different Filters into
            // multiple inputs of the Join.
            return None
          }
        }
      }
    }
    inputWithCorrectFields
  }
}
