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
