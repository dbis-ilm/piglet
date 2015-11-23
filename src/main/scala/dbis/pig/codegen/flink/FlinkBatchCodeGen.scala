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
package dbis.pig.codegen.flink

import dbis.pig.expr.RefExprExtractor
import dbis.pig.op._
import dbis.pig.schema._
import dbis.pig.plan.DataflowPlan
import scala.collection.mutable.ArrayBuffer
import dbis.pig.expr.DerefTuple
import dbis.pig.expr.Ref
import dbis.pig.expr.Predicate
import dbis.pig.expr.Expr
import dbis.pig.expr.Func
import dbis.pig.expr.NamedField
import dbis.pig.expr.PositionalField
import dbis.pig.codegen.CodeGenerator
import dbis.pig.codegen.spark.BatchCodeGen
import dbis.pig.codegen.TemplateException
import scala.collection.mutable.ListBuffer

class FlinkBatchCodeGen(template: String) extends BatchCodeGen(template) {

  /*------------------------------------------------------------------------------------------------- */
  /*                                  Scala-specific code generators                                  */
  /*------------------------------------------------------------------------------------------------- */

  /**
   * Generates code for the GROUPING Operator
   *
   * @param node the GROUPING operator
   * @param groupExpr the grouping expression
   * @return the Scala code implementing the GROUPING operator
   */
  override def emitGrouping(node: Grouping, groupExpr: GroupingExpression): String = {
    require(node.schema.isDefined)
    val className = schemaClassName(node.schema.get.className)

    // GROUP ALL: no need to generate a key 
    if (groupExpr.keyList.isEmpty)
      callST("groupBy", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "class" -> className))
    else {
      val keyExtr = if (groupExpr.keyList.size > 1) {
        // the grouping key consists of multiple fields, i.e. we have
        // to construct a tuple where the type is the TupleType of the group field
        val field = node.schema.get.field("group")
        val className = field.fType match {
          case TupleType(f, c) => schemaClassName(c)
          case _               => throw TemplateException("unknown type for grouping key")
        }

        s"${className}(" + groupExpr.keyList.map(e => emitRef(node.inputSchema, e, "itr.head")).mkString(",") + ")"
      } else groupExpr.keyList.map(e => emitRef(node.inputSchema, e, "itr.head")).mkString // the simple case: the key is a single field

      callST("groupBy", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "class" -> className,
        "expr" -> emitGroupExpr(node.inputSchema, groupExpr),
        "keyExtr" -> keyExtr))
    }
  }

  /**
   * Generates code for the ORDERBY Operator
   *
   * @param node the OrderBy operator node
   * @param spec Order specification
   * @return the Scala code implementing the ORDERBY operator
   */
  override def emitOrderBy(node: PigOperator, spec: List[OrderBySpec]): String = {
    val key = spec.map(spec => getOrderIndex(node.schema, spec.field))
    val orders = spec.map(spec => if (spec.dir == OrderByDirection.AscendingOrder) "ASCENDING" else "DESCENDING")
    callST("orderBy", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "key" -> key, "asc" -> orders))
  }

  /**
   * Generates code for the ACCUMULATE operator
   *
   * @param node the ACCUMULATE operator
   * @param gen the generator expressions containing the aggregates
   * @return the Scala code implementing the operator
   */
  override def emitAccumulate(node: PigOperator, gen: GeneratorList): String = {
    val inputSchemaDefined = node.inputSchema.isDefined
    require(node.schema.isDefined)
    val outClassName = schemaClassName(node.schema.get.className)
    var initAggrFun: String = ""
    var moreAggrFuns: ListBuffer[String] = new ListBuffer()
    val updExpr = gen.exprs.zipWithIndex.map {
      case (e, i) =>
        require(e.expr.isInstanceOf[Func])
        val funcName = e.expr.asInstanceOf[Func].f.toUpperCase

        val traverse = new RefExprExtractor
        e.expr.traverseAnd(null, traverse.collectRefExprs)
        val refExpr = traverse.exprs.head

        val str: String = refExpr.r match {
          case nf @ NamedField(n, _) => s"$node.inputSchema.get.indexOfField(nf)"
          case PositionalField(p)    => if (inputSchemaDefined) s"$p" else "0"
          case _                     => ""
        }
        if (i == 0) initAggrFun = (funcName + "," + str) else moreAggrFuns += (funcName + "," + str)
    }

    callST("accumulate", Map("out" -> node.outPipeName,
      "in" -> node.inPipeName,
      "class" -> outClassName,
      "init_aggr_expr" -> initAggrFun,
      "more_aggr_expr" -> moreAggrFuns))
  }

  def getOrderIndex(schema: Option[Schema],
                    ref: Ref): Int = schema match {

    case Some(s) => ref match {
      case nf @ NamedField(f, _) => s.indexOfField(nf)
      case PositionalField(pos)  => pos
      case _                     => 0
    }
    case None =>
      // if we don't have a schema this is not allowed
      throw new TemplateException(s"the flink orderby operator needs a schema, thus, invalid field ")
  }

  private def printQuote(values: List[String]) = """""""+ values.mkString("""","""") + """""""
  /**
   * Generates code for the JOIN operator.
   *
   * @param node the JOIN operator node
   * @param rels list of Pipes to join
   * @param exprs list of join keys
   * @return the Scala code implementing the JOIN operator
   */
  override def emitJoin(node: PigOperator, rels: List[Pipe], exprs: List[List[Ref]]): String = {
    require(node.schema.isDefined)

    val res = node.inputs.zip(exprs)
    val keys = res.map { case (i, k) => k.map { x => s"_${getOrderIndex(i.producer.schema, x)}" }}
    var keysGroup: ListBuffer[(List[String], List[String])] = new ListBuffer
    for (i <- 0 until keys.length -1) {
      val v = (keys(i), keys(i+1))
      keysGroup += v
    }
    val keysGroup1 =  keysGroup.zipWithIndex.map {case (i,k) => 
      if (k > 0) 
        (printQuote(i._1.map { x => s"_$k.$x" }),  printQuote(i._2))
      else
        (printQuote(i._1), printQuote(i._2))
    } 
    val keys1 = keysGroup1.map( x =>x._1 ) 
    val keys2 = keysGroup1.map( x =>x._2 )
    
    val className = node.schema match {
      case Some(s) => schemaClassName(s.className)
      case None    => schemaClassName(node.outPipeName)
    }
    var pairs = "(v1,v2)"
    for (i <- 3 to rels.length) {
      pairs = s"($pairs,v$i)"
    }
    val fieldList = ArrayBuffer[String]()
    for (i <- 1 to node.inputs.length) {
      node.inputs(i - 1).producer.schema match {
        case Some(s) => fieldList ++= s.fields.zipWithIndex.map { case (f, k) => s"v$i._$k" }
        case None    => fieldList += s"v$i._0"
      }
    }
    callST("join",
      Map("out" -> node.outPipeName,
        "rel1" -> rels.head.name,
        "class" -> className,
        "rels" -> rels.tail.map(_.name),
        "pairs" -> pairs,
        "rel1_keys" ->keys1,
        "rel2_keys" ->keys2,
        "fields" -> fieldList.mkString(", ")))
  }

  /*------------------------------------------------------------------------------------------------- */
  /*                           implementation of the GenCodeBase interface                            */
  /*------------------------------------------------------------------------------------------------- */

  /**
   * Generate code for the given Pig operator.
   *
   * @param node the operator (an instance of PigOperator)
   * @return a string representing the code
   */
  override def emitNode(node: PigOperator): String = {
    node match {
      case op @ Grouping(out, in, groupExpr, _) => emitGrouping(op, groupExpr)
      case OrderBy(out, in, orderSpec, _)       => emitOrderBy(node, orderSpec)
      case Accumulate(out, in, gen)             => emitAccumulate(node, gen)
      case Join(out, rels, exprs, _)            => emitJoin(node, node.inputs, exprs)
      case _                                    => super.emitNode(node)
    }
  }

}

class FlinkBatchGenerator(templateFile: String) extends CodeGenerator {
  override val codeGen = new FlinkBatchCodeGen(templateFile)
}

