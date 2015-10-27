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
    * @return
    */
  override def emitRef(schema: Option[Schema], ref: Ref,
                       tuplePrefix: String = "t",
                       aggregate: Boolean = false,
                       namedRef: Boolean = false): String = ref match {
      case DerefTuple(r1, r2) => 
        if (aggregate)
          s"${emitRef(schema, r1, "t")}.map(e => e${emitRef(tupleSchema(schema, r1), r2, "")})"
        else
          s"${emitRef(schema, r1, "t")}${emitRef(tupleSchema(schema, r1), r2, "", aggregate, namedRef)}"
      case _ => super.emitRef(schema, ref, tuplePrefix, aggregate, namedRef)
  }

  /**
    * Generates Scala code for a nested plan, i.e. statements within nested FOREACH.
    *
    * @param parent the parent FOREACH statement
    * @param plan the dataflow plan representing the nested statements
    * @return the generated code
    */
  def emitNestedPlan(parent: PigOperator, plan: DataflowPlan): String = {
    val schema = parent.inputSchema

    require(parent.schema.isDefined)
    val className = schemaClassName(parent.schema.get.className)

    "{\n" + plan.operators.map {
      case Generate(expr) => s"""${className}(${emitGenerator(schema, expr, namedRef = true)})"""
      case n@ConstructBag(out, ref) => ref match {
        case DerefTuple(r1, r2) => {
          val p1 = findFieldPosition(schema, r1)
          val p2 = findFieldPosition(tupleSchema(schema, r1), r2)
          require(p1 >= 0 && p2 >= 0)
          s"""val ${n.outPipeName} = t._$p1.map(l => l._$p2).toList"""
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
    // val requiresPlainFlatten =  node.asInstanceOf[Foreach].containsFlatten(onBag = false)
    val requiresFlatMap = node.asInstanceOf[Foreach].containsFlatten(onBag = true)
    gen match {
      case GeneratorList(expr) => {
        if (requiresFlatMap)
          emitBagFlattenGenerator(node, expr)
        else
          emitGenerator(node.inputSchema, expr)
      }
      case GeneratorPlan(plan) => {
        val subPlan = node.asInstanceOf[Foreach].subPlan.get
        emitNestedPlan(node, subPlan)
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
    * @param node the FILTER operator
    * @param pred the filter predicate
    * @return the Scala code implementing the FILTER operator
    */
  def emitFilter(node: PigOperator, pred: Predicate): String = {
    callST("filter", Map("out" -> node.outPipeName,"in" -> node.inPipeName,
      "pred" -> emitPredicate(node.schema, pred)))
  }

  /** 
    * Generates code for the FOREACH Operator
    *
    * @param node the FOREACH Operator node
    * @param gen the generate expression
    * @return the Scala code implementing the FOREACH operator
    */
  def emitForeach(node: PigOperator, gen: ForeachGenerator): String = {
    require(node.schema.isDefined)
    val className = schemaClassName(node.schema.get.className)
    println("emitForeach: className = " + className)
    val expr = emitForeachExpr(node, gen)
    // in case of a nested FOREACH the tuples are creates as part of the GENERATE clause
    // -> no need to give the schema class
    if (gen.isInstanceOf[GeneratorPlan]) {
      callST("foreachNested", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "expr" -> expr))
    }
    else {
      // we need to know if the generator contains flatten on tuples or on bags (which require flatMap)
      val requiresFlatMap = node.asInstanceOf[Foreach].containsFlatten(onBag = true)
      if (requiresFlatMap)
        callST("foreachFlatMap", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "expr" -> expr))
      else
        callST("foreach", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "class" -> className, "expr" -> expr))
    }
  }

  /**
    * Generates code for the GROUPING Operator
    *
    * @param node the GROUPING operator
    * @param groupExpr the grouping expression
    * @return the Scala code implementing the GROUPING operator
    */
  def emitGrouping(node: Grouping, groupExpr: GroupingExpression): String = {
    require(node.schema.isDefined)
    val className = schemaClassName(node.schema.get.className)

    // GROUP ALL: no need to generate a key
    if (groupExpr.keyList.isEmpty)
      callST("groupBy", Map("out"->node.outPipeName, "in"->node.inPipeName, "class" -> className))
    else {
      val keyExtr = if (groupExpr.keyList.size > 1) {
        // the grouping key consists of multiple fields, i.e. we have
        // to construct a tuple where the type is the TupleType of the group field
        val field = node.schema.get.field("group")
        val className = field.fType match {
          case TupleType(f, c) => schemaClassName(c)
          case _ => throw TemplateException("unknown type for grouping key")
        }

        s"${className}(" + (for (i <- 1 to groupExpr.keyList.size) yield s"k._$i").mkString(", ") + ")"
      }
      else "k" // the simple case: the key is a single field

      callST("groupBy", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "class" -> className,
        "expr" -> emitGroupExpr(node.inputSchema, groupExpr),
        "keyExtr" -> keyExtr))
    }
  }

  /**
    * Generates code for the JOIN operator.
    *
    * @param node the JOIN operator node
    * @param rels list of Pipes to join
    * @param exprs list of join keys
    * @return the Scala code implementing the JOIN operator
    */
  def emitJoin(node: PigOperator, rels: List[Pipe], exprs: List[List[Ref]]): String = {
    require(node.schema.isDefined)

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
     * And finally, create the join kv vars for them...
     */
    var str = callST("join_key_map", Map("rels" -> drels.map(_.name), "keys" -> dkeys))

    /*
     * We construct a string v._0, v._1 ... w._0, w._1 ...
     * The numbers of v's and w's are determined by the size of the input schemas.
     */
    val vsize = rels.head.inputSchema.get.fields.length
    val fieldList = node.schema.get.fields.zipWithIndex
      .map{case (f, i) => if (i < vsize) s"v._$i" else s"w._${i - vsize}"}.mkString(", ")
    println("join fields: " + fieldList)

    val className = node.schema match {
      case Some(s) => schemaClassName(s.className)
      case None => schemaClassName(node.outPipeName)
    }

    /*
      *  ...as well as the actual join.
      */
    str += callST("join",
      Map("out" -> node.outPipeName,
        "rel1" -> rels.head.name,
        "class" -> className,
        "key1" -> keys.head,
        "rel2" -> rels.tail.map(_.name),
        "key2" -> keys.tail,
        "fields" -> fieldList))

    joinKeyVars += rels.head.name
    joinKeyVars ++= rels.tail.map(_.name)
    str
  }

  /**
    * Generates code for the LIMIT Operator
    *
    * @param node the LIMIT operator
    * @param num amount of retruned records
    * @return the Scala code implementing the LIMIT operator
    */
  def emitLimit(node: PigOperator, num: Int): String = {
    callST("limit", Map("out" -> node.outPipeName, "in" -> node.inPipeName, "num" -> num))
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
      case Filter(out, in, pred, _) => emitFilter(node, pred)
      case Foreach(out, in, gen, _) => emitForeach(node, gen)
      case op@Grouping(out, in, groupExpr, _) => emitGrouping(op, groupExpr)
      case Join(out, rels, exprs, _) => emitJoin(node, node.inputs, exprs)
      case Limit(out, in, num) => emitLimit(node, num)
      case OrderBy(out, in, orderSpec, _) => emitOrderBy(node, node.outPipeName, node.inPipeName, orderSpec)
      case _ => super.emitNode(node)
    }
  }
}

class BatchCompile(templateFile: String) extends Compile {
  override val codeGen = new BatchGenCode(templateFile)
}

