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
import dbis.pig.schema.Schema
import scala.collection.immutable.Map
import scala.collection.mutable.Set
import org.clapper.scalasti.STGroupFile
import dbis.pig.expr.{Expr, Value}

/**
 * An exception representing an error in handling the templates for code generation.
 *
 * @param msg the error message
 */
case class TemplateException(msg: String) extends Exception(msg)


trait CodeGeneratorBase {

  /**
   * The name of the template file used for code generation.
   */
  var templateFile: String = null

  /**
   * A map of alias names for user-defined functions.
   */
  var udfAliases: Option[Map[String, (String, List[Any])]] = None

  /**
   * The set of _KV variables refering to RDDs which are created for joins.
   *
   */
  val joinKeyVars = Set[String]()

  /**
   * Generate code for a class representing a schema type.
   *
   * @param schema the schema for which we generate a class
   * @return a string representing the code
   */
  def emitSchemaClass(schema: Schema): String

  /**
   * Generate code for the given Pig operator.
   *
   * @param node the operator (an instance of PigOperator)
   * @return a string representing the code
   */
  def emitNode(node: PigOperator): String

  /**
   * Generate code needed for importing packages, classes, etc.
   *
   * @return a string representing the import code
   */
  def emitImport(additionalImports: Option[String] = None): String

  /**
   * Generate code for the header of the script outside the main class/object,
   * e.g. defining the main object.
   *
   * @param scriptName the name of the script (e.g. used for the object)
   * @param additionalCode source code (Scala, C++) that was embedded into the script
   * @return a string representing the header code
   */
  def emitHeader1(scriptName: String, additionalCode: String): String

  /**
   * Generate code for the header of the script which should be defined inside
   * the main class/object.
   *
   * @param scriptName the name of the script (e.g. used for the object)
   * @param enableProfiling add profiling code to the generated code
   * @return a string representing the header code
   */
  def emitHeader2(scriptName: String, enableProfiling: Boolean): String

  /**
   * Generate code needed for finishing the script.
   *
   * @return a string representing the end of the code.
   */
  def emitFooter: String

  /**
   * Generate code for any helper class/function if needed by the given operator.
   *
   * @param node the Pig operator requiring helper code
   * @return a string representing the helper code
   */
  def emitHelperClass(node: PigOperator): String
  
  def emitStageIdentifier(line: Int, lineage: String): String
  /*------------------------------------------------------------------------------------------------- */
  /*                               template handling code                                             */
  /*------------------------------------------------------------------------------------------------- */

  /** 
    * Invoke a given string template without parameters.
    *
    * @param template the name of the string template
    * @return the text from the template
    */
  def callST(template: String): String = callST(template, Map[String, Any]())

  /** 
    * Invoke a given string template with a map of key-value pairs used for replacing
    * the keys in the template by the string values.
    *
    * @param template the name of the string template
    * @param attributes the map of key-value pairs
    * @return the text from the template
    */
  def callST(template: String, attributes: Map[String, Any]): String = { 
    val group = STGroupFile(templateFile)
    val tryST = group.instanceOf(template)
    if (tryST.isSuccess) {
      val st = tryST.get
      if (attributes.nonEmpty) {
        attributes.foreach {
          attr => st.add(attr._1, attr._2)
        }
      }   
      st.render()
    } else throw TemplateException(s"Template '$template' not implemented or not found")
  }

}

/**
 * Defines the interface to the code generator. A concrete class
 * has to override only the codeGen function which should return the
 * actual code generator object for the given target.
 */
trait CodeGenerator {
  /**
   * Return the code generator object for the given target.
   *
   * @return an instance of the code generator.
   */
  def codeGen: CodeGeneratorBase

  /**
   * Generates a string containing the code for the given dataflow plan.
   *
   * @param scriptName the name of the Pig script.
   * @param plan the dataflow plan.
   * @param forREPL generate code for the Scala/Spark interactive REPL, i.e. without
   *                Header2 and Footer
   * @return the string representation of the code
   */
  def compile(scriptName: String, plan: DataflowPlan, profiling: Boolean, forREPL: Boolean = false): String = {
    require(codeGen != null, "code generator undefined")

    if (plan.udfAliases != null) {
      codeGen.udfAliases = Some(plan.udfAliases.toMap)
    }

    var additionalImports: Option[String] = None
    if (plan.checkExpressions(Expr.containsMatrixType)) {
      println("------------------- MATRIX contained ---------------")
      additionalImports = Some("import breeze.linalg._")
    }

    // generate import statements
    var code = codeGen.emitImport(additionalImports)

    if (!forREPL)
      code = code + codeGen.emitHeader1(scriptName, plan.code)

    // generate schema classes for all registered types and schemas
    for (schema <- Schema.schemaList) {
      code = code + codeGen.emitSchemaClass(schema)
    }
      
    // generate helper classes (if needed, e.g. for custom key classes)
    for (n <- plan.operators) {
      
      val genCode = codeGen.emitHelperClass(n)
      code = code + genCode  + (if(genCode.nonEmpty) "\n" else "")
    }

    if (!forREPL)
      // generate the object definition representing the script
      code = code + codeGen.emitHeader2(scriptName, profiling)

    for (n <- plan.operators) {
      val generatedCode = codeGen.emitNode(n)
      
      if(profiling) {
        /* count the generated lines
         * this is needed for the PerfMonitor to identify stages by line number
         * 
         * +1 is for the additional line that is inserted for the register code
         */
        val lines = scala.io.Source.fromBytes((code + generatedCode).getBytes).getLines().size + 1
        
        // register an operation with its line number and lineage  
        val registerIdCode = codeGen.emitStageIdentifier(lines, n.lineageSignature)

        code = code + registerIdCode + "\n"
        
      }
      code =  code + generatedCode + "\n"
    }

    // generate the cleanup code
    if (forREPL) code else code + codeGen.emitFooter
  }
}
