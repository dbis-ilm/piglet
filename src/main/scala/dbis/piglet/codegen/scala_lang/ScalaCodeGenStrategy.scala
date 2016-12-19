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
package dbis.piglet.codegen.scala_lang

import java.net.URI

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenStrategy, CodeGenTarget}
import dbis.piglet.expr.Expr
import dbis.piglet.op.PigOperator
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.schema._
import dbis.piglet.tools.Conf

import scala.collection.mutable.ListBuffer
import dbis.piglet.op.Load
import dbis.piglet.op.Filter
import dbis.piglet.op.Limit
import dbis.piglet.tools.logging.PigletLogging
import dbis.piglet.op.Foreach
import dbis.piglet.op.Distinct
import dbis.piglet.op.Sample
import dbis.piglet.op.Grouping
import dbis.piglet.op.Union
import dbis.piglet.op.OrderBy
import dbis.piglet.op.Top
import dbis.piglet.op.Accumulate
import dbis.piglet.op.Join
import dbis.piglet.op.Cross
import dbis.piglet.op.Dump
import dbis.piglet.op.Empty
import dbis.piglet.op.Store
import dbis.piglet.op.SpatialJoin
import dbis.piglet.op.SpatialFilter
import dbis.piglet.codegen.spark.SpatialFilterEmitter
import dbis.piglet.codegen.spark.SpatialJoinEmitter

abstract class ScalaCodeGenStrategy extends CodeGenStrategy with PigletLogging {
  // initialize target and emitters
  val target = CodeGenTarget.Unknown

//  val pkg = "dbis.piglet.op"
//  def emitters[O <: PigOperator]: Map[String, CodeEmitter[PigOperator]] = Map[String, CodeEmitter[PigOperator]](
//    s"$pkg.Load" -> new LoadEmitter,
//    s"$pkg.Filter" -> new FilterEmitter,
//    s"$pkg.Limit" -> new LimitEmitter,
//    s"$pkg.Foreach" -> new ForeachEmitter,
//    s"$pkg.Distinct" -> new DistinctEmitter,
//    s"$pkg.Sample" -> new SampleEmitter,
//    s"$pkg.Union" -> new UnionEmitter,
//    s"$pkg.Grouping" -> new GroupingEmitter,
//    s"$pkg.OrderBy" -> new OrderByEmitter,
//    s"$pkg.Top" -> new TopEmitter,
//    s"$pkg.Accumulate" -> new AccumulateEmitter,
//    s"$pkg.Join" -> new JoinEmitter,
//    s"$pkg.Cross" -> new CrossEmitter,
//    s"$pkg.Dump" -> new DumpEmitter,
//    s"$pkg.Empty" -> new EmptyEmitter,
//    s"$pkg.Store" -> new StoreEmitter
//  )
  

  override def collectAdditionalImports(plan: DataflowPlan) = {
    val additionalImports = ListBuffer.empty[String]
    if (plan.checkExpressions(Expr.containsMatrixType)) {
      additionalImports += "import breeze.linalg._"
    }

    additionalImports
  }

  def emitterForNode[O <: PigOperator](op: O): CodeEmitter[O] = {
    val em = op match {
      case _: Load => new LoadEmitter
      case _: Filter => new FilterEmitter
      case _: Limit => new LimitEmitter
      case _: Foreach => new ForeachEmitter
      case _: Distinct => new DistinctEmitter
      case _: Sample => new SampleEmitter
      case _: Union => new UnionEmitter
      case _: Grouping => new GroupingEmitter
      case _: OrderBy => new OrderByEmitter
      case _: Top => new TopEmitter
      case _: Accumulate => new AccumulateEmitter
      case _: Join => new JoinEmitter
      case _: Cross => new CrossEmitter
      case _: Dump => new DumpEmitter
      case _: Empty => new EmptyEmitter
      case _: Store => new StoreEmitter
      case _ => throw new IllegalArgumentException(s"no emitter for $op")      
    }
  
    em.asInstanceOf[CodeEmitter[O]]
  }

  /**
    * Generate code for embedded code: usually this code is just copied
    * to the generated file.
    *
    * @param additionalCode the code to be embedded
    * @return a string representing the code
    */
  def emitEmbeddedCode(ctx: CodeGenContext, additionalCode: String) = additionalCode

  def emitNode[O <: PigOperator](ctx: CodeGenContext, node: O): String = {
    val className = node.getClass.getName
    
    val emitter = emitterForNode(node)
    
    logger.debug(s"using ${emitter.getClass.getName} for op $className")
    
    var code = emitter.beforeCode(ctx, node)
    if (code.length > 0) code += "\n"

    code += emitter.code(ctx, node)

    val afterCode = emitter.afterCode(ctx, node)
    if (afterCode.length > 0)
      code += "\n" + afterCode

    code
  }


  /**
    * Generate code for classes representing schema types.
    *
    * @param schemas the list of schemas for which we generate classes
    * @return a string representing the code
    */
  override def emitSchemaHelpers(ctx: CodeGenContext, schemas: List[Schema]): String = {
    var converterCode = ""

    val classes = ListBuffer.empty[(String, String)]

    for (schema <- schemas) {
      val values = ScalaEmitter.createSchemaInfo(schema)

      classes += ScalaEmitter.emitSchemaClass(values)
      converterCode += ScalaEmitter.emitSchemaConverters(values)
    }

    val p = "_t([0-9]+)_Tuple".r

    val sortedClasses = classes.sortWith { case (left, right) =>
      val leftNum = left._1 match {
        case p(group) => group.toInt
        case _ => throw new IllegalArgumentException(s"unexpected class name: $left")
      }

      val rightNum = right._1 match {
        case p(group) => group.toInt
        case _ => throw new IllegalArgumentException(s"unexpected class name: $left")
      }

      leftNum < rightNum
    }

    val classCode = sortedClasses.map(_._2).mkString("\n")

    classCode + "\n" + converterCode
  }

  /**
    * Generate code for any helper class/function if needed by the given operator.
    *
    * @param node the Pig operator requiring helper code
    * @return a string representing the helper code
    */
  override def emitHelperClass(ctx: CodeGenContext, node: PigOperator): String = {
    val emitter = emitterForNode(node)

    emitter.helper(ctx, node)
  }
}
