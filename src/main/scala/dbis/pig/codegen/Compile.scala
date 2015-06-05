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
package dbis.pig.codegen

import dbis.pig.op.PigOperator
import dbis.pig.plan.DataflowPlan

/**
 * Created by kai on 08.04.15.
 */
trait GenCodeBase {
  def emitNode(node: PigOperator): String
  def emitImport: String
  def emitHeader(scriptName: String): String
  def emitFooter: String
  def emitHelperClass(node: PigOperator): String

  // def emitPredicate(schema: Option[Schema], predicate: Predicate): String
  // def emitRef(schema: Option[Schema], ref: Ref): String
  // def emitGrouping(schema: Option[Schema], groupingExpr: GroupingExpression): String
}

/**
 * Defines the interface to the code generator. A concrete class
 * has to override only the codeGen function which should return the
 * actual code generator object for the given target.
 */
trait Compile {
  /**
   * Return the code generator object for the given target.
   *
   * @return an instance of the code generator.
   */
  def codeGen: GenCodeBase

  /**
   * Generates a string containing the code for the given dataflow plan.
   *
   * @param scriptName the name of the Pig script.
   * @param plan the dataflow plan.
   * @return the string representation of the code
   */
  def compile(scriptName: String, plan: DataflowPlan): String = {
    require(codeGen != null, "code generator undefined")
    // generate import statements
    var code = codeGen.emitImport

    // generate helper classes (if needed, e.g. for custom key classes)
    for (n <- plan.operators) {
      code = code + codeGen.emitHelperClass(n) + "\n"
    }

    // generate the object definition representing the script
    code = code + codeGen.emitHeader(scriptName)
    for (n <- plan.operators) {
      code = code + codeGen.emitNode(n) + "\n"
    }

    // generate the cleanup code
    code + codeGen.emitFooter
  }
}
