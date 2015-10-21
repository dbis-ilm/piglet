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

import dbis.pig.op._
import dbis.pig.schema._
import dbis.pig.plan.DataflowPlan


class BatchGenCode(template: String) extends ScalaBackendGenCode(template) {


  /*------------------------------------------------------------------------------------------------- */
  /*                                  Scala-specific code generators                                  */
  /*------------------------------------------------------------------------------------------------- */

  /**
    *
    * @param schema
    * @param tuplePrefix
    * @param requiresTypeCast
    * @return
    */
  override def emitRef(schema: Option[Schema], ref: Ref, tuplePrefix: String = "t",
                       requiresTypeCast: Boolean = true,
                       aggregate: Boolean = false): String = ref match {
      case DerefTuple(r1, r2) => 
        if (aggregate) s"${emitRef(schema, r1, "t", false)}.asInstanceOf[Seq[List[Any]]].map(e => e${emitRef(tupleSchema(schema, r1), r2, "", false)})"
        else s"${emitRef(schema, r1, "t", false)}.asInstanceOf[List[Any]]${emitRef(tupleSchema(schema, r1), r2, "", false, aggregate)}"
      case _ => super.emitRef(schema, ref, tuplePrefix, requiresTypeCast, aggregate)
  }

  /**
    * Generates Scala code for a nested plan, i.e. statements within nested FOREACH.
    *
    * @param schema the input schema of the FOREACH statement
    * @param plan the dataflow plan representing the nested statements
    * @return the generated code
    */
  def emitNestedPlan(schema: Option[Schema], plan: DataflowPlan): String = {
    "{\n" + plan.operators.map {
      case Generate(expr) => s"""( ${emitGenerator(schema, expr)} )"""
      case n@ConstructBag(out, ref) => ref match {
        case DerefTuple(r1, r2) => {
          val p1 = findFieldPosition(schema, r1)
          val p2 = findFieldPosition(tupleSchema(schema, r1), r2)
          require(p1 >= 0 && p2 >= 0)
          s"""val ${n.outPipeName} = t($p1).asInstanceOf[Seq[Any]].map(l => l.asInstanceOf[Seq[Any]]($p2))"""
        }
        case _ => "" // should not happen
      }
      case n@Distinct(out, in, _) => callST("distinct", Map("out"->n.outPipeName,"in"->n.inPipeName))
      case n@Filter(out, in, pred, _) => callST("filter", Map("out" -> n.outPipeName, "in" -> n.inPipeName, "pred" -> emitPredicate(n.schema, pred)))
      case n@Limit(out, in, num) => callST("limit", Map("out" -> n.outPipeName, "in" -> n.inPipeName, "num" -> num))
      case OrderBy(out, in, orderSpec, _) => "" // TODO!!!!
      case _ => ""
    }.mkString("\n") + "}"
  }

  def emitForeachExpr(node: PigOperator, gen: ForeachGenerator): String = {
    // we need to know if the generator contains flatten on tuples or on bags (which require flatMap)
    val requiresPlainFlatten =  node.asInstanceOf[Foreach].containsFlatten(onBag = false)
    val requiresFlatMap = node.asInstanceOf[Foreach].containsFlatten(onBag = true)
    gen match {
      case GeneratorList(expr) => {
        if (requiresFlatMap) emitBagFlattenGenerator(node.inputSchema, expr)
          else {
          if (requiresPlainFlatten) emitFlattenGenerator(node.inputSchema, expr)
            else emitGenerator(node.inputSchema, expr)
        }
      }
      case GeneratorPlan(plan) => {
        val subPlan = node.asInstanceOf[Foreach].subPlan.get
        emitNestedPlan(node.inputSchema, subPlan)
      }
    }
  }

  /*------------------------------------------------------------------------------------------------- */
  /*                                   Node code generators                                           */
  /*------------------------------------------------------------------------------------------------- */

  /**
    * Generates code for the CROSS operator.
    *
    * @param node the Cross Operator node
    * @return the Scala code implementing the CROSS operator
    */

   def emitCross(node: PigOperator): String = {
    val rels = node.inputs
    callST("cross", Map("out" -> node.outPipeName,"rel1"->rels.head.name,"rel2"->rels.tail.map(_.name)))
  }

  /**
    * Generates code for the DISTINCT Operator
    *
    * @param node the DISTINCT operator
     * @return the Scala code implementing the DISTINCT operator
    */
  def emitDistinct(node: PigOperator): String = {
    callST("distinct", Map("out" -> node.outPipeName, "in" -> node.inPipeName))
  }

  /** 
    * Generates code for the FILTER Operator
    *
    * @param schema the nodes schema
    * @param out name of the output bag
    * @param in name of the input bag
    * @param pred the filter predicate
    * @return the Scala code implementing the FILTER operator
    */
  def emitFilter(schema: Option[Schema], out: String, in: String, pred: Predicate): String = { 
    callST("filter", Map("out"->out,"in"->in,"pred"->emitPredicate(schema, pred)))
  }

  /** 
    * Generates code for the FOREACH Operator
    *
    * @param node the FOREACH Operator node
    * @param out name of the output bag
    * @param in name of the input bag
    * @param gen the generate expression
    * @return the Scala code implementing the FOREACH operator
    */
  def emitForeach(node: PigOperator, out: String, in: String, gen: ForeachGenerator): String = { 
    // we need to know if the generator contains flatten on tuples or on bags (which require flatMap)

    val expr = emitForeachExpr(node, gen)

    val requiresFlatMap = node.asInstanceOf[Foreach].containsFlatten(onBag = true)
    if (requiresFlatMap)
      callST("foreachFlatMap", Map("out"->out,"in"->in,"expr"->expr))
    else
      callST("foreach", Map("out"->out,"in"->in,"expr"->expr))
  }

  /**
    * Generates code for the GROUPING Operator
    *
    * @param node the GROUPING operator
    * @param groupExpr the grouping expression
    * @return the Scala code implementing the GROUPING operator
    */
  def emitGrouping(node: PigOperator, groupExpr: GroupingExpression): String = {
    val className = node.schema match {
      case Some(s) => schemaClassName(s.element.name)
      case None => "TextLine"
    }
    if (groupExpr.keyList.isEmpty)
      callST("groupBy", Map("out"->node.outPipeName, "in"->node.inPipeName, "class" -> className))
    else {
      val keyExtr = if (groupExpr.keyList.size > 1)
        "(" + (for (i <- 1 to groupExpr.keyList.size) yield s"k._$i").mkString(", ") + ")"
      else "k"

      callST("groupBy", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "class" -> className,
        "expr" -> emitGroupExpr(node.inputSchema, groupExpr),
        "keyExtr" -> keyExtr))
    }
  }

  /**
    * Generates code for the JOIN operator.
    *
    * @param node the Join Operator node
    * @param out name of the output bag
    * @param rels list of Pipes to join
    * @param exprs list of join keys
    * @return the Scala code implementing the JOIN operator
    */
  def emitJoin(node: PigOperator, out: String, rels: List[Pipe], exprs: List[List[Ref]]): String = {
    val res = node.inputs.zip(exprs)
    val keys = res.map{case (i,k) => emitJoinKey(i.producer.schema, k)}

    /*
     * We don't generate key-value RDDs which we have already created and registered in joinKeyVars.
     * Thus, we build a list of 1 and 0's where 1 stands for a relation name for which we have already
     * created a _kv variable.
     */
    val duplicates = rels.map(r => if (joinKeyVars.contains(r.name)) 1 else 0)

    /*
     * Now we build lists for rels and keys by removing the elements corresponding to 1's in the duplicate
     * list.
     */
    val drels = rels.zipWithIndex.filter{r => duplicates(r._2) == 0}.map(_._1)
    val dkeys = keys.zipWithIndex.filter{k => duplicates(k._2) == 0}.map(_._1)

    /*
     * And finally, create the join kv vars for them.
     */
    var str = callST("join_key_map", Map("rels"->drels.map(_.name), "keys"->dkeys))
    str += callST("join", Map("out"->out,"rel1"->rels.head.name,"key1"->keys.head,"rel2"->rels.tail.map(_.name),"key2"->keys.tail))

    joinKeyVars += rels.head.name
    joinKeyVars ++= rels.tail.map(_.name)
    str
  }

  /**
    * Generates code for the LIMIT Operator
    *
    * @param out name of the output bag
    * @param in name of the input bag
    * @param num amount of retruned records
    * @return the Scala code implementing the LIMIT operator
    */
  def emitLimit(out: String, in: String, num: Int): String = {
    callST("limit", Map("out"->out,"in"->in,"num"->num))
  }

  /**
    * Generates code for the ORDERBY Operator
    *
    * @param node the OrderBy Operator node
    * @param out name of the output bag
    * @param in name of the input bag
    * @param spec Order specification
    * @return the Scala code implementing the ORDERBY operator
    */
  def emitOrderBy(node: PigOperator, out: String, in: String, spec: List[OrderBySpec]): String = {
    val key = emitSortKey(node.schema, spec, out, in)
    val asc = ascendingSortOrder(spec.head)
    callST("orderBy", Map("out"->out,"in"->in,"key"->key,"asc"->asc))
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
      /*
       * NOTE: Don't use "out" here -> it refers only to initial constructor argument but isn't consistent
       *       after changing the pipe name. Instead, use node.outPipeName
       */
      case Cross(out, rels, _) => emitCross(node)
      case Distinct(out, in, _) => emitDistinct(node)
      case Filter(out, in, pred, _) => emitFilter(node.schema, node.outPipeName, node.inPipeName, pred)
      case Foreach(out, in, gen, _) => emitForeach(node, node.outPipeName, node.inPipeName, gen)
      case Grouping(out, in, groupExpr, _) => emitGrouping(node, groupExpr)
      case Join(out, rels, exprs, _) => emitJoin(node, node.outPipeName, node.inputs, exprs)
      case Limit(out, in, num) => emitLimit(node.outPipeName, node.inPipeName, num)
      case OrderBy(out, in, orderSpec, _) => emitOrderBy(node, node.outPipeName, node.inPipeName, orderSpec)
      case _ => super.emitNode(node)
    }
  }
}

class BatchCompile(templateFile: String) extends Compile {
  override val codeGen = new BatchGenCode(templateFile)
}

