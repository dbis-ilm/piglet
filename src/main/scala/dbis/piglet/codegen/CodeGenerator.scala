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
package dbis.piglet.codegen

import dbis.piglet.Piglet.Lineage
import dbis.piglet.op.{PigOperator, TimingOp}
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.schema.Schema

import scala.collection.immutable.Map
//import scala.collection.mutable.Set
import java.net.URI

import dbis.piglet.tools.TopoSort
import dbis.setm.SETM.timing

import scala.collection.mutable.ListBuffer


/**
  * Defines the interface to the actual code generator. For each backend a concrete
  * class implementing this interface completely has to be provided.
  */
trait CodeGenStrategy {

  /**
    * The target for which the code is generated.
    */
  def target: CodeGenTarget.Value

//  def emitters: Map[String, CodeEmitter[PigOperator]]
//
//  def emitterForNode(node: String): CodeEmitter[PigOperator] = {
//    if (!emitters.contains(node))
//      throw new CodeGenException(s"invalid plan operator: $node")
//    emitters(node)
//  }
  
  def emitterForNode[O <: PigOperator](op: O): CodeEmitter[O]

  def collectAdditionalImports(plan: DataflowPlan): Seq[String]

  /**
   * Generate code for classes representing schema types.
   * @param ctx The generator context
   * @param schemas the list of schemas for which we generate classes
   * @param profiling Flag to enable profiling code
   * @return a string representing the code
   */
  def emitSchemaHelpers(ctx: CodeGenContext, schemas: List[Schema], profiling: Boolean): String

  /**
   * Generate code for the given Pig operator.
   *
   * @param node the operator (an instance of PigOperator)
   * @return a string representing the code
   */
  def emitNode[O <: PigOperator](ctx: CodeGenContext, node: O): String

  /**
   * Generate code needed for importing packages, classes, etc.
   *
   * @param additionalImports a list of strings representing required imports
   * @return a string representing the import code
   */
  def emitImport(ctx: CodeGenContext, additionalImports: Seq[String] = Seq.empty): String

  /**
   * Generate code for the header of the script outside the main class/object,
   * e.g. defining the main object.
   *
   * @param scriptName the name of the script (e.g. used for the object)
   * @return a string representing the header code
   */
  def emitHeader1(ctx: CodeGenContext, scriptName: String): String

  /**
    * Generate code for embedded code: usually this code is just copied
    * to the generated file.
    *
    * @param additionalCode the code to be embedded
    * @return a string representing the code
    */
  def emitEmbeddedCode(ctx: CodeGenContext, additionalCode: String): String

  /**
   * Generate code for the header of the script which should be defined inside
   * the main class/object.
   *
   * @param scriptName the name of the script (e.g. used for the object)
   * @param profiling add profiling code to the generated code
   * @return a string representing the header code
   */
  def emitHeader2(ctx: CodeGenContext, scriptName: String, profiling: Option[URI], operators:Seq[Lineage]=Seq.empty): String

  /**
   * Generate code needed for finishing the script.
   *
   * @param plan the dataflow plan for which we generate the code
   * @return a string representing the end of the code.
   */
  def emitFooter(ctx: CodeGenContext, plan: DataflowPlan, profiling: Option[URI], operators: Seq[Lineage] = Seq.empty): String

  /**
   * Generate code for any helper class/function if needed by the given operator.
   *
   * @param node the Pig operator requiring helper code
   * @return a string representing the helper code
   */
  def emitHelperClass(ctx: CodeGenContext, node: PigOperator): String
}

/**
 * Defines the interface to the code generator. A concrete class
 * has to override only the codeGen function which should return the
 * actual code generator object for the given target.
 */
class CodeGenerator(codeGen: CodeGenStrategy) {
  /**
   * Return the code generator object for the given target.
   *
   * @return an instance of the code generator.
   */
  /**
   * Generates a string containing the code for the given dataflow plan.
   *
   * @param scriptName the name of the Pig script.
   * @param plan the dataflow plan.
   * @param forREPL generate code for the Scala/Spark interactive REPL, i.e. without
   *                Header2 and Footer
   * @return the string representation of the code
   */
  def generate(scriptName: String, plan: DataflowPlan, profiling: Option[URI], forREPL: Boolean = false): String = timing("generate code") {
    require(codeGen != null, "code generator undefined")


    CodeEmitter.profiling = profiling

    val aliases = if (plan.udfAliases != null) { Some(plan.udfAliases.toMap) } else None

    val ctx = CodeGenContext(codeGen.target, aliases)

    val additionalImports = codeGen.collectAdditionalImports(plan)

    // generate import statements
    var code = codeGen.emitImport(ctx, additionalImports)

    if (!forREPL)
      code = code + codeGen.emitHeader1(ctx, scriptName)
      
    if(plan.code.nonEmpty)
      code = code + codeGen.emitEmbeddedCode(ctx, plan.code)

    code += codeGen.emitSchemaHelpers(ctx, Schema.schemaList(), profiling.isDefined)

    val lineages = ListBuffer.empty[Lineage]

    // generate helper classes (if needed, e.g. for custom key classes)
    for (op <- plan.operators) {

      op match {
        case _: TimingOp =>
        case _ => lineages += op.lineageSignature
      }

      try {
        val genCode = codeGen.emitHelperClass(ctx, op)
        code = code + genCode  + (if(genCode.nonEmpty) "\n" else "")
      } catch {
        case e: CodeGenException => throw new CodeGenException(s"error producing helper class for $op", e)
      }
    }
    
    if (!forREPL)
      // generate the object definition representing the script
      code = code + codeGen.emitHeader2(ctx, scriptName, profiling, lineages)

    val sortedOps = TopoSort(plan)
      
    sortedOps.foreach { op =>
//    for(op <- plan.operators) {
      
      try {
        
        val generatedCode = codeGen.emitNode(
            CodeGenContext(ctx, Map("schema" -> op.schema, "tuplePrefix" -> "t")),
            op)
  
        code =  code + generatedCode + "\n"
        
      } catch {
      case e: CodeGenException => 
        op.printOperator(2)
        throw new CodeGenException(s"error producing code for $op", e)
      }
    }

    // generate the cleanup code
    if (forREPL) code else code + codeGen.emitFooter(ctx, plan, profiling,lineages)
  }
}

object CodeGenerator {
  def apply(codeGenStrategy: CodeGenStrategy) = new CodeGenerator(codeGenStrategy)
}
