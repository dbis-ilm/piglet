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

import com.typesafe.scalalogging.LazyLogging

import dbis.pig.op._
import dbis.pig.plan.DataflowPlan
import dbis.pig.schema._
import dbis.pig.udf._

import java.net.URI
import scala.collection.mutable.ListBuffer


// import scala.collection.mutable.Map

/**
 * Implements a code generator for Scala-based backends such as Spark or Flink which use
 * a template file for the backend-specific code.
 *
 * The main idea of the generated code is the following: a record of the backend-specific data
 * structure (e.g. RDD in Spark) is represented by List[Any] to allow variable-sized tuples as
 * supported in Pig. This requires to always map the output of any backend transformation back
 * to such a list.
 *
 * @param template the name of the backend-specific template fle
 */
abstract class ScalaBackendGenCode(template: String) extends GenCodeBase with LazyLogging {

   templateFile = template 
  /*------------------------------------------------------------------------------------------------- */
  /*                                           helper functions                                       */
  /*------------------------------------------------------------------------------------------------- */

  /**
   *
   * @param schema
   * @param ref
   * @return
   */
  def tupleSchema(schema: Option[Schema], ref: Ref): Option[Schema] = {
    val tp = ref match {
      case NamedField(f) => schema match {
        case Some(s) => s.field(f).fType
        case None => throw new SchemaException(s"unknown schema for field $f")
      }
      case PositionalField(p) => schema match {
        case Some(s) => s.field(p).fType
        case None => None
      }
      case _ => None
    }
    if (tp == None)
      None
    else
      Some(new Schema( if (tp.isInstanceOf[BagType]) tp.asInstanceOf[BagType] else BagType(tp.asInstanceOf[TupleType])))
  }

  /**
   * Replaces Pig-style quotes (') by double quotes in a string.
   *
   * @param s a quoted string in Pig-style (')
   * @return a string with double quotes
   */
  def quote(s: String): String = s.replace('\'', '"')

  /**
   * Returns true if the sort order of the OrderBySpec is ascending
   *
   * @param spec the OrderBySpec value
   * @return true if sorting in ascending order
   */
  def ascendingSortOrder(spec: OrderBySpec): String = if (spec.dir == OrderByDirection.AscendingOrder) "true" else "false"


  // TODO: complex types
  val scalaTypeMappingTable = Map[PigType, String](
    Types.IntType -> "Int",
    Types.LongType -> "Long",
    Types.FloatType -> "Float",
    Types.DoubleType -> "Double",
    Types.CharArrayType -> "String",
    Types.ByteArrayType -> "String")

  /**
   * Returns the name of the Scala type for representing the given field. If the schema doesn't exist we assume
   * bytearray which is mapped to String.
   *
   * @param field a Ref representing the field (positional or named=
   * @param schema the schema of the field
   * @return the name of the Scala type
   */
  def scalaTypeOfField(field: Ref, schema: Option[Schema]) : String = {
    schema match {
      case Some(s) => {
        field match {
          case PositionalField(f) => scalaTypeMappingTable(s.field(f).fType)
          case NamedField(n) => scalaTypeMappingTable(s.field(n).fType)
          case _ => "String"
        }
      }
      case None => "String"
    }
  }

  /**
   *
   * @param schema
   * @return
   */
  def listToTuple(schema: Option[Schema]): String = schema match {
    case Some(s) => '"' + (0 to s.fields.length-1).toList.map(i => if (s.field(i).isBagType) s"{%s}" else s"%s").mkString(",") + '"' +
      ".format(" + (0 to s.fields.length-1).toList.map(i =>
      if (s.field(i).isBagType)
        s"""t($i).asInstanceOf[Seq[Any]].mkString(",")"""
      else
        s"t($i)").mkString(",") + ")"
    case None => s"t(0)"
  }

  /**
   * Find the index of the field represented by the reference in the given schema.
   * The reference could be a named field or a positional field. If not found -1 is returned.
   *
   * @param schema the schema containing the field
   * @param field the field denoted by a Ref object
   * @return the index of the field
   */
  def findFieldPosition(schema: Option[Schema], field: Ref): Int = field match {
    case NamedField(f) => schema match {
      case Some(s) => s.indexOfField(f)
      case None => -1
    }
    case PositionalField(p) => p
    case _ => -1
  }


  /*------------------------------------------------------------------------------------------------- */
  /*                                  Scala-specific code generators                                  */
  /*------------------------------------------------------------------------------------------------- */

   /**
   *
   * @param schema
   * @param ref
   * @param tuplePrefix
   * @param requiresTypeCast
   * @return
   */
  def emitRef(schema: Option[Schema], ref: Ref, tuplePrefix: String = "t", requiresTypeCast: Boolean = true, aggregate: Boolean = false): String = ref match {
    case NamedField(f) => schema match {
      case Some(s) => {
        val idx = s.indexOfField(f)
        if (idx == -1) {
          // TODO: field name not found, for now we assume that it refers to a bag (relation)
          f
        }
        else {
          if (requiresTypeCast) {
            val field = s.field(idx)
            val typeCast = if (field.fType.isInstanceOf[BagType]) s"List" else scalaTypeMappingTable(field.fType)

            /* if we need to cast to anything else than string (i.e. int, double, etc) we have to
             * cast to string before, because if we load from a BinStorage, we only get Any types
             * that do not have a toInt or toDouble conversion method
             */
            s"${tuplePrefix}(${idx}).asInstanceOf[${typeCast}]"
          }
          else
            s"${tuplePrefix}(${idx})"
        }
      }
      case None => throw new SchemaException(s"unknown schema for field $f")
    } // TODO: should be position of field
    case PositionalField(pos) =>
      if (requiresTypeCast && schema.isDefined) {
        val field = schema.get.field(pos)
        val typeCast = if (field.fType.isInstanceOf[BagType]) s"List" else scalaTypeMappingTable(field.fType)
        s"$tuplePrefix($pos).asInstanceOf[${typeCast}]"
      }
      else s"$tuplePrefix($pos)"
    case Value(v) => v.toString
    // case DerefTuple(r1, r2) => s"${emitRef(schema, r1)}.asInstanceOf[List[Any]]${emitRef(schema, r2, "")}"
    // case DerefTuple(r1, r2) => s"${emitRef(schema, r1, "t", false)}.asInstanceOf[List[Any]]${emitRef(tupleSchema(schema, r1), r2, "", false)}"
    case DerefMap(m, k) => s"${emitRef(schema, m)}.asInstanceOf[Map[String,Any]](${k})"
    case _ => { "" }
  }

  /**
   * Generate Scala code for a predicate on expressions.
   *
   * @param schema the optional input schema of the operator where the expressions refer to.
   * @param predicate the actual predicate
   * @return a string representation of the generated Scala code
   */
  def emitPredicate(schema: Option[Schema], predicate: Predicate): String = predicate match {
    case Eq(left, right) => { s"${emitExpr(schema, left)} == ${emitExpr(schema, right)}"}
    case Neq(left, right) => { s"${emitExpr(schema, left)} != ${emitExpr(schema, right)}"}
    case Leq(left, right) => { s"${emitExpr(schema, left)} <= ${emitExpr(schema, right)}"}
    case Lt(left, right) => { s"${emitExpr(schema, left)} < ${emitExpr(schema, right)}"}
    case Geq(left, right) => { s"${emitExpr(schema, left)} >= ${emitExpr(schema, right)}"}
    case Gt(left, right) => { s"${emitExpr(schema, left)} > ${emitExpr(schema, right)}"}
    case And(left, right) => s"${emitPredicate(schema, left)} && ${emitPredicate(schema, right)}"
    case Or(left, right) => s"${emitPredicate(schema, left)} || ${emitPredicate(schema, right)}"
    case Not(pred) => s"!(${emitPredicate(schema, pred)})"
    case PPredicate(pred) => s"(${emitPredicate(schema, pred)})"
    case _ => { s"UNKNOWN PREDICATE: $predicate" }
  }

  /**
   * Generates Scala code for a grouping expression in GROUP BY. We construct code for map
   * in the form "map(t => {(t(0),t(1),...)}" if t(0), t(1) are grouping attributes.
   *
   * @param schema the optional input schema of the operator where the expressions refer to.
   * @param groupingExpr the actual grouping expression object
   * @return a string representation of the generated Scala code
   */
  def emitGroupExpr(schema: Option[Schema], groupingExpr: GroupingExpression): String = {
    if (groupingExpr.keyList.size == 1)
      groupingExpr.keyList.map(e => emitRef(schema, e, requiresTypeCast = false)).mkString
    else
      "(" + groupingExpr.keyList.map(e => emitRef(schema, e, requiresTypeCast = false)).mkString(",") + ")"
  }

  /**
   *
   * @param schema
   * @param joinExpr
   * @return
   */
  def emitJoinKey(schema: Option[Schema], joinExpr: List[Ref]): String = {
    if (joinExpr.size == 1)
      emitRef(schema, joinExpr.head)
    else
      s"Array(${joinExpr.map(e => emitRef(schema, e)).mkString(",")}).mkString"
  }

  /**
   *
   * @param schema
   * @param expr
   * @param requiresTypeCast
   * @return
   */
  def emitExpr(schema: Option[Schema], expr: ArithmeticExpr, requiresTypeCast: Boolean = true, aggregate: Boolean = false): String = expr match {
    case CastExpr(t, e) => {
      // TODO: check for invalid type
      val targetType = scalaTypeMappingTable(t)
      s"${emitExpr(schema, e)}.to$targetType"
    }
    case PExpr(e) => s"(${emitExpr(schema, e)})"
    case MSign(e) => s"-${emitExpr(schema, e)}"
    case Add(e1, e2) => s"${emitExpr(schema, e1)} + ${emitExpr(schema, e2)}"
    case Minus(e1, e2) => s"${emitExpr(schema, e1)} - ${emitExpr(schema, e2)}"
    case Mult(e1, e2) => s"${emitExpr(schema, e1)} * ${emitExpr(schema, e2)}"
    case Div(e1, e2) => s"${emitExpr(schema, e1)} / ${emitExpr(schema, e2)}"
    case RefExpr(e) => s"${emitRef(schema, e, "t", requiresTypeCast, aggregate)}"
    case Func(f, params) => {
      val pTypes = params.map(p => {p.resultType(schema)._2})
      UDFTable.findUDF(f, pTypes) match {
        case Some(udf) => { 
          if (udf.isAggregate) {
            if (!f.equalsIgnoreCase("count"))
              s"${udf.scalaName}(${emitExpr(schema, params.head, false, true)}.map(e => e.asInstanceOf[Number].doubleValue))"
            else
               s"${udf.scalaName}(${emitExpr(schema, params.head, false, true)}.asInstanceOf[Seq[Any]])"  
          }
          else s"${udf.scalaName}(${params.map(e => emitExpr(schema, e)).mkString(",")})"
        }
        case None => {
          // check if we have have an alias in DataflowPlan
          if (udfAliases.nonEmpty && udfAliases.get.contains(f)) {
            val alias = udfAliases.get(f)
            val paramList = alias._2 ::: params.map(e => emitExpr(schema, e))
            s"${alias._1}(${paramList.mkString(",")})"
          }
          else {
            // we don't know the function yet, let's assume there is a corresponding Scala function
            s"$f(${params.map(e => emitExpr(schema, e)).mkString(",")})"
          }
        }
      }
    }
    case FlattenExpr(e) => emitExpr(schema, e, false)
    case ConstructTupleExpr(exprs) => s"PigFuncs.toTuple(${exprs.map(e => emitExpr(schema, e)).mkString(",")})"
    case ConstructBagExpr(exprs) => s"PigFuncs.toBag(${exprs.map(e => emitExpr(schema, e)).mkString(",")})"
    case ConstructMapExpr(exprs) => s"PigFuncs.toMap(${exprs.map(e => emitExpr(schema, e)).mkString(",")})"
    case _ => println("unsupported expression: " + expr); ""
  }

  /**
   *
   * @param schema
   * @param genExprs
   * @return
   */
  def emitGenerator(schema: Option[Schema], genExprs: List[GeneratorExpr]): String = {
    s"List(${genExprs.map(e => emitExpr(schema, e.expr, false)).mkString(",")})"
  }

  /**
   * Constructs the GENERATE expression list in FOREACH for cases where this list contains the FLATTEN
   * operator. This case requires to unwrap the list in flatten.
   *
   * @param schema
   * @param genExprs
   * @return
   */
  def emitFlattenGenerator(schema: Option[Schema], genExprs: List[GeneratorExpr]): String = {
    s"PigFuncs.flatTuple(${emitGenerator(schema, genExprs)})"
  }

  /**
   *
   * @param schema
   * @param genExprs
   * @return
   */
  def emitBagFlattenGenerator(schema: Option[Schema], genExprs: List[GeneratorExpr]): String = {
    val flattenExprs = genExprs.filter(e => e.expr.traverseOr(schema.getOrElse(null), Expr.containsFlattenOnBag))
    val otherExprs = genExprs.diff(flattenExprs)
    if (flattenExprs.size == 1) {
      val ex = flattenExprs.head.expr
      if (otherExprs.nonEmpty)
        s"List(${emitExpr(schema, ex, false)}.asInstanceOf[Seq[Any]].map(s => (${otherExprs.map(e => emitExpr(schema, e.expr, false))}, s))"
      else
        s"${emitExpr(schema, ex, false)}"
    }
    else
      s"" // i.flatMap(t => t(1).asInstanceOf[Seq[Any]].map(s => List(t(0),s)))
  }

  /**
   *
   * @param schema the input schema of the operator
   * @param params the list of parameters (as Refs)
   * @return the generated code
   */
  def emitParamList(schema: Option[Schema], params: Option[List[Ref]]): String = params match {
    case Some(refList) => s",${refList.map(r => emitRef(schema, r)).mkString(",")}"
    case None => ""
  }

  /**
   *
   * @param schema
   * @param orderSpec
   * @param out
   * @param in
   * @return
   */
  def emitSortKey(schema: Option[Schema], orderSpec: List[OrderBySpec], out: String, in: String) : String = {
    if (orderSpec.size == 1)
      emitRef(schema, orderSpec.head.field)
    else
      s"custKey_${out}_${in}(${orderSpec.map(r => emitRef(schema, r.field)).mkString(",")})"
  }

  def emitHelperClass(node: PigOperator): String = {
    def genCmpExpr(col: Int, num: Int) : String =
      if (col == num) s"{ this.c$col compare that.c$col }"
      else s"{ if (this.c$col == that.c$col) ${genCmpExpr(col+1, num)} else this.c$col compare that.c$col }"

    /**
     * Generates a function for producing a string representation of a tuple. This is needed because
     * Spark's saveAsTextFile simply calls the toString method which results in "List(...)" or
     * "CompactBuffer(...)" in the output.
     *
     * @param schema a schema describing the tuple structure
     * @return the string representation
     */
    def genStringRepOfTuple(schema: Option[Schema], stringDelim: String = "','"): String = schema match {
      case Some(s) => (0 to s.fields.length-1).toList.map{ i => s.field(i).fType match {
          // TODO: this should be processed recursively
        case BagType(n, t) => s""".append(t(${i}).map(s => s.mkString("(", ",", ")")).mkString("{", ",", "}"))"""
        case TupleType(n, f) => s""".append(t(${i}).map(s => s.toString).mkString("(", ",", ")"))"""
        case MapType(t, n) => s""".append(t(${i}).asInstanceOf[Map[String,Any]].map{case (k,v) => k + "#" + v}.mkString("[", ",", "]"))"""
        case _ => s".append(t($i))"
      }}.mkString(s"\n    .append($stringDelim)\n")
      case None => s".append(t(0))\n"
    }

    node match {
      case OrderBy(out, in, orderSpec, _) => {
        var params = Map[String,Any]()
        //Spark
        params += "cname" -> s"custKey_${node.outPipeName}_${node.inPipeName}"
        var col = 0
        params += "fields" -> orderSpec.map(o => { col += 1; s"c$col: ${scalaTypeOfField(o.field, node.schema)}" }).mkString(", ")
        params += "cmpExpr" -> genCmpExpr(1, orderSpec.size)

        //Flink
        params += "out"->node.outPipeName
        params += "key"->orderSpec.map(r => emitRef(node.schema, r.field)).mkString(",")
        if (ascendingSortOrder(orderSpec.head) == "false") params += "reverse"->true

        callST("orderHelper", Map("params"->params))
      }
      case Store(in, file,_,params) => { s"""
        |def tuple${node.inPipeName}ToString(t: List[Any]): String = {
        |  implicit def anyToSeq(a: Any) = a.asInstanceOf[Seq[Any]]
        |
        |  val sb = new StringBuilder
        |  sb${genStringRepOfTuple(node.schema, if(params != null && params.isDefinedAt(0)) params(0) else "','")}
        |  sb.toString
        |}""".stripMargin
      }
      case _ => ""
    }
  }


  /*------------------------------------------------------------------------------------------------- */
  /*                                   Node code generators                                           */
  /*------------------------------------------------------------------------------------------------- */
 
  private def getFieldTypes(s: Schema): String = {
      val fieldList = s.fields.zipWithIndex
      val castList = fieldList.map { case (f, i) => ( 
          if (f.fType == Types.CharArrayType || f.fType == Types.ByteArrayType) 
            s"t($i)" 
          else  
            s"t($i).to${scalaTypeMappingTable(f.fType)}"
      ) }
      castList.mkString(",")
  }
  /**
   * Generates code for the LOAD operator.
   *
   * @param out name of the output bag
   * @param file the name of the file to be loaded
   * @param loaderFunc an optional loader function (we assume a corresponding Scala function is available)
   * @param loaderParams an optional list of parameters to a loader function (e.g. separators)
   * @return the Scala code implementing the LOAD operator
   */
  def emitLoader(out: String, file: URI, loaderFunc: String, loaderParams: List[String]): String = {
    if (loaderFunc == "")
      callST("loader", Map("out"->out,"file"->file.toString()))
    else {
      val params = if (loaderParams != null && loaderParams.nonEmpty) ", " + loaderParams.mkString(",") else ""
      callST("loader", Map("out"->out,"file"->file.toString(),"func"->loaderFunc,"params"->params))
    }
  }
  
  /**
   * Generates code for the LOAD operator.
   *
   * @param node the load node operator itself
   * @param file the name of the file to be loaded
   * @param loaderFunc an optional loader function (we assume a corresponding Scala function is available)
   * @param loaderParams an optional list of parameters to a loader function (e.g. separators)
   * @return the Scala code implementing the LOAD operator
   */
  def emitLoad(node: PigOperator, file: URI, loaderFunc: String, loaderParams: List[String]): String = {
    node.schema match {
      case Some(s) => {
        val op = emitLoader(s"${node.outPipeName}_", file, loaderFunc, loaderParams) 
        op +"\n" +callST("foreach", Map("out"->node.outPipeName,"in"->s"${node.outPipeName}_","expr"->s"List(${getFieldTypes(s)})"))
      }
      case None => {
        emitLoader(node.outPipeName, file, loaderFunc, loaderParams)
      }
    }   
  }

  /**
   * Generates code for the STORE operator.
   *
   * @param in name of the input bag
   * @param file the URI of the target file
   * @param func a storage function
   * @return the Scala code implementing the STORE operator
   */
  def emitStore(in: String, file: URI, func: String): String = {
    
    /* for BinStorage we do not convert the tuple into a string because
     * we want to keep our fields as is
     */
    if(func == "BinStorage")
      callST("store", Map("in"->in,"file"->file.toString(),"func"->func))
    else
      callST("store", Map("in"->in,"file"->file.toString(),"schema"->s"tuple${in}ToString(t)","func"->func))
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
  def emitNode(node: PigOperator): String = {
    node match {
        /*
         * NOTE: Don't use "out" here -> it refers only to initial constructor argument but isn't consistent
         *       after changing the pipe name. Instead, use node.outPipeName
         */
      case Load(out, file, schema, func, params) => emitLoad(node, file, func, params)
      case Dump(in) => callST("dump", Map("in"->node.inPipeName))
      case Store(in, file, func, params) => emitStore(node.inPipeName, file, func)
      case Describe(in) => s"""println("${node.schemaToString}")"""
      case SplitInto(in, splits) => callST("splitInto", Map("in"->node.inPipeName, "out"->node.outPipeNames, "pred"->splits.map(s => emitPredicate(node.schema, s.expr))))
      case Union(out, rels) => callST("union", Map("out"->node.outPipeName,"in"->node.inPipeName,"others"->node.inPipeNames.tail))
      case Sample(out, in, expr) => callST("sample", Map("out"->node.outPipeName,"in"->node.inPipeName,"expr"->emitExpr(node.schema, expr)))
      case StreamOp(out, in, op, params, schema) => callST("streamOp", Map("out"->node.outPipeName,"op"->op,"in"->node.inPipeName,"params"->emitParamList(node.schema, params)))
      case HdfsCmd(cmd, params) => callST("fs", Map("cmd"->cmd, "params"->params))
      case Empty(_) => ""
      case _ => throw new TemplateException(s"Template for node '$node' not implemented or not found")
    }
  }

   /**
   * Generate code needed for importing required Scala packages.
   *
   * @return a string representing the import code
   */
  def emitImport: String = callST("init_code")

  /**
   * Generate code for the header of the script outside the main class/object,
   * i.e. defining the main object.
   *
   * @param scriptName the name of the script (e.g. used for the object)
   * @param additionalCode Scala source code that was embedded into the script
   * @return a string representing the header code
   */
  def emitHeader1(scriptName: String, additionalCode: String = ""): String =
    callST("query_object", Map("name" -> scriptName, "embedded_code" -> additionalCode))

  /**
   *
   * Generate code for the header of the script which should be defined inside
   * the main class/object.
   *
   * @param scriptName the name of the script (e.g. used for the object)
   * @return a string representing the header code
   */
  def emitHeader2(scriptName: String): String = callST("begin_query", Map("name" -> scriptName))

  /**
   * Generate code needed for finishing the script and starting the execution.
   *
   * @return a string representing the end of the code.
   */
  def emitFooter: String = callST("end_query", Map("name" -> "Starting Query"))

}
