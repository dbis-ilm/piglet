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
import dbis.pig.backends.BackendManager
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
 * structure (e.g. RDD in Spark) is represented by an instance of a specific case class which is
 * generated according to the schema of an operator.
 *
 * @param template the name of the backend-specific template fle
 */
abstract class ScalaBackendGenCode(template: String) extends GenCodeBase with LazyLogging {

   templateFile = template 
  /*------------------------------------------------------------------------------------------------- */
  /*                                           helper functions                                       */
  /*------------------------------------------------------------------------------------------------- */

  def schemaClassName(name: String) = s"_${name}_Tuple"

  /**
   *
   * @param schema
   * @param ref
   * @return
   */
  def tupleSchema(schema: Option[Schema], ref: Ref): Option[Schema] = {
    val tp = ref match {
      case nf @ NamedField(f, _) => schema match {
        case Some(s) => if (f == s.element.name) s.element.valueType else s.field(nf).fType
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
          case nf @ NamedField(_, _) => scalaTypeMappingTable(s.field(nf).fType)
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
    case nf @ NamedField(f, _) => schema match {
      case Some(s) => if (f == s.element.name) 0 else s.indexOfField(nf)
      case None => -1
    }
    case PositionalField(p) => p
    case _ => -1
  }


  /*------------------------------------------------------------------------------------------------- */
  /*                                  Scala-specific code generators                                  */
  /*------------------------------------------------------------------------------------------------- */

   /**
    * Generate Scala code for a reference to a named field, a positional field or a tuple/map derefence.
    *
    * @param schema the (optional) schema decribing the tuple structure
    * @param ref
    * @param tuplePrefix the variable name
    * @param aggregate ??
    * @return the generated code
    */
  def emitRef(schema: Option[Schema],
              ref: Ref,
              tuplePrefix: String = "t",
              aggregate: Boolean = false,
              namedRef: Boolean = false): String = ref match {
    case nf @ NamedField(f, _) => if (namedRef) {
      // check if f exists in the schema
      schema match {
        case Some(s) => {
          val p = s.indexOfField(nf)
          if (p != -1)
            s"$tuplePrefix._$p"
          else
            f // TODO: check whether thus is a valid field (or did we check it already in checkSchemaConformance??)
        }
        case None => throw new TemplateException(s"invalid field name $f") // if we don't have a schema this is not allows
      }
    }
    else
        s"$tuplePrefix._${schema.get.indexOfField(nf)}" // s"$tuplePrefix.$f"
    case PositionalField(pos) => s"$tuplePrefix._$pos"
    case Value(v) => v.toString
    // case DerefTuple(r1, r2) => s"${emitRef(schema, r1)}.asInstanceOf[List[Any]]${emitRef(schema, r2, "")}"
    // case DerefTuple(r1, r2) => s"${emitRef(schema, r1, "t", false)}.asInstanceOf[List[Any]]${emitRef(tupleSchema(schema, r1), r2, "", false)}"
    case DerefMap(m, k) => s"${emitRef(schema, m)}(${k})"
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
      groupingExpr.keyList.map(e => emitRef(schema, e)).mkString
    else
      "(" + groupingExpr.keyList.map(e => emitRef(schema, e)).mkString(",") + ")"
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
   * @return
   */
  def emitExpr(schema: Option[Schema],
               expr: ArithmeticExpr,
               aggregate: Boolean = false,
               namedRef: Boolean = false): String = expr match {
    case CastExpr(t, e) => {
      // TODO: check for invalid type
      val targetType = scalaTypeMappingTable(t)
      s"${emitExpr(schema, e, namedRef = namedRef)}.to$targetType"
    }
    case PExpr(e) => s"(${emitExpr(schema, e, namedRef = namedRef)})"
    case MSign(e) => s"-${emitExpr(schema, e, namedRef = namedRef)}"
    case Add(e1, e2) => s"${emitExpr(schema, e1, namedRef = namedRef)} + ${emitExpr(schema, e2, namedRef = namedRef)}"
    case Minus(e1, e2) => s"${emitExpr(schema, e1, namedRef = namedRef)} - ${emitExpr(schema, e2, namedRef = namedRef)}"
    case Mult(e1, e2) => s"${emitExpr(schema, e1, namedRef = namedRef)} * ${emitExpr(schema, e2, namedRef = namedRef)}"
    case Div(e1, e2) => s"${emitExpr(schema, e1, namedRef = namedRef)} / ${emitExpr(schema, e2, namedRef = namedRef)}"
    case RefExpr(e) => s"${emitRef(schema, e, "t", aggregate, namedRef = namedRef)}"
    case Func(f, params) => {
      val pTypes = params.map(p => p.resultType(schema))
      UDFTable.findUDF(f, pTypes) match {
        case Some(udf) => { 
          if (udf.isAggregate) {
            s"${udf.scalaName}(${emitExpr(schema, params.head, aggregate = true, namedRef = namedRef)})"
          }
          else s"${udf.scalaName}(${params.map(e => emitExpr(schema, e, namedRef = namedRef)).mkString(",")})"
        }
        case None => {
          // check if we have have an alias in DataflowPlan
          if (udfAliases.nonEmpty && udfAliases.get.contains(f)) {
            val alias = udfAliases.get(f)
            val paramList = alias._2 ::: params.map(e => emitExpr(schema, e, namedRef = namedRef))
            s"${alias._1}(${paramList.mkString(",")})"
          }
          else {
            // we don't know the function yet, let's assume there is a corresponding Scala function
            s"$f(${params.map(e => emitExpr(schema, e, namedRef = namedRef)).mkString(",")})"
          }
        }
      }
    }
    case FlattenExpr(e) => flattenExpr(schema, e)
    case ConstructTupleExpr(exprs) => s"_???_Tuple(${exprs.map(e => emitExpr(schema, e, namedRef = namedRef)).mkString(",")})"
    case ConstructBagExpr(exprs) => s"List(${exprs.map(e => s"_???_Tuple(${emitExpr(schema, e, namedRef = namedRef)})").mkString(",")})"
    case ConstructMapExpr(exprs) => s"Map[String,???](${exprs.map(e => emitExpr(schema, e, namedRef = namedRef)).mkString(",")})"
    case _ => println("unsupported expression: " + expr); ""
  }

  /**
   * Constructs the Scala code for flattening a tuple. We have to determine the field in the
   * input schema refering to a tuple type and extract all fields of this tuple type.
   *
   * @param schema the input schema of the FOREACH operator
   * @param e the expression to be flattened (should be a RefExpr)
   * @return a string representation of the Scala code
   */
  def flattenExpr(schema: Option[Schema], e: ArithmeticExpr): String = {
    if (schema.isEmpty) throw new TemplateException("cannot flatten a tuple without a schema")
    // we need the field name used in Scala (either the actual name or _<position>) as well as
    // the actual field
    val (refName, field) = e match {
      case RefExpr(r) => r match {
        case nf@NamedField(n, _) => ("_" + schema.get.indexOfField(nf), schema.get.field(nf))
        case PositionalField(p) => ("_" + p.toString, schema.get.field(p))
          // either a named or a positional field: all other cases are not allowed!?
        case _ => throw new TemplateException("invalid flatten expression: argument isn't a reference")
      }
      case _ => throw new TemplateException(s"invalid flatten expression: argument $e isn't a reference")
    }
    if (field.fType.tc != TypeCode.TupleType)
      // make sure it is really a tuple type: other types cannot be flattened
      throw new TemplateException("invalid flatten expression: argument doesn't refer to a tuple")
    val tupleType = field.fType.asInstanceOf[TupleType]
    // finally, produce a list of t.<refName>.<fieldPos>
    tupleType.fields.zipWithIndex.map{ case (f, i) => s"t.${refName}._$i"}.mkString(", ")
  }

  /**
   * Constructs the GENERATE expression list in FOREACH.
   *
   * @param schema the input schema
   * @param genExprs the list of expressions in the GENERATE clause
   * @return a string representation of the Scala code
   */
  def emitGenerator(schema: Option[Schema], genExprs: List[GeneratorExpr], namedRef: Boolean = false): String = {
    s"${genExprs.map(e => emitExpr(schema, e.expr, aggregate = false, namedRef = namedRef)).mkString(", ")}"
  }

  /**
   * Creates the Scala code needed for a flatten expression where the argument is a bag.
   * It requires a flatMap transformation.
   *
   * @param schema the optional input schema
   * @param genExprs the list of generator expressions
   * @return a string representation of the Scala code
   */
  def emitBagFlattenGenerator(schema: Option[Schema], genExprs: List[GeneratorExpr]): String = {
    // extract the flatten expression from the generator list
    val flattenExprs = genExprs.filter(e => e.expr.traverseOr(schema.getOrElse(null), Expr.containsFlattenOnBag))
    // determine the remaining expressions
    val otherExprs = genExprs.diff(flattenExprs)
    if (flattenExprs.size == 1) {
      // there is only a single flatten expression
      val ex: FlattenExpr = flattenExprs.head.expr.asInstanceOf[FlattenExpr]
      if (otherExprs.nonEmpty)
        // we have to cross join the flatten expression with the others
        s"???(${emitExpr(schema, ex)}.map(s => (${otherExprs.map(e => emitExpr(schema, e.expr))}, s))"
      else {
        // there is no other expression: we just construct an expression for flatMap
        println("----> " + ex.a)
        s"${emitExpr(schema, ex.a)}"
      }
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
      case _ => ""
    }
  }


  /*------------------------------------------------------------------------------------------------- */
  /*                                   Node code generators                                           */
  /*------------------------------------------------------------------------------------------------- */
 
  /**
   * Generates code for the LOAD operator.
   *
   * @param node the load node operator itself
   * @param file the name of the file to be loaded
   * @param loaderFunc an optional loader function (we assume a corresponding Scala function is available)
   * @param loaderParams an optional list of parameters to a loader function (e.g. separators)
   * @return the Scala code implementing the LOAD operator
   */
  def emitLoad(node: PigOperator, file: URI, loaderFunc: Option[String], loaderParams: List[String]): String = {
    def schemaExtractor(schema: Schema): String =
      schema.fields.zipWithIndex.map{case (f, i) => s"data($i).to${scalaTypeMappingTable(f.fType)}"}.mkString(", ")

    var paramMap = Map("out" -> node.outPipeName, "file" -> file.toString)
    if (loaderFunc.isEmpty)
      paramMap += ("func" -> BackendManager.backend.defaultConnector)
    else {
      paramMap += ("func" -> loaderFunc.get)
      if (loaderParams != null && loaderParams.nonEmpty)
        paramMap += ("params" -> loaderParams.mkString(","))
    }
    node.schema match {
      case Some(s) => paramMap += ("extractor" ->
        s"""(data: Array[String]) => ${schemaClassName(s.className)}(${schemaExtractor(s)})""",
        "class" -> schemaClassName(s.className))
      case None => paramMap += ("extractor" -> "(data: Array[String]) => TextLine(data(0))",
        "class" -> "TextLine")
    }
    callST("loader", paramMap)
  }

  /**
   * Generates code for the STORE operator.
   *
   * @param node the STORE operator
   * @param file the URI of the target file
   * @param storeFunc a storage function
   * @param params an (optional) parameter list for the storage function
   * @return the Scala code implementing the STORE operator
   */
  def emitStore(node: PigOperator, file: URI, storeFunc: Option[String], params: List[String]): String = {
    var paramMap = Map("in" -> node.inPipeName,
      "file" -> file.toString,
      "func" -> storeFunc.getOrElse(BackendManager.backend.defaultConnector))
    node.schema match {
      case Some(s) => paramMap += ("class" -> schemaClassName(s.className))
      case None => paramMap += ("class" -> "TextLine")
    }

    if (params != null && params.nonEmpty)
      paramMap += ("params" -> params.mkString(","))

    callST("store", paramMap)
  }


  /*------------------------------------------------------------------------------------------------- */
  /*                           implementation of the GenCodeBase interface                            */
  /*------------------------------------------------------------------------------------------------- */

  /**
   * Generate code for a class representing a schema type.
   *
   * @param schema the schema for which we generate a class
   * @return a string representing the code
   */
  def emitSchemaClass(schema: Schema): String = {
    def typeName(f: PigType, n: String) = scalaTypeMappingTable.get(f) match {
      case Some(n) => n
      case None => f match {
        // if we have a bag without a name then we assume that we have got
        // a case class with _<field_name>_Tuple
        case BagType(v) => s"Iterable[_${v.className}_Tuple]"
        case TupleType(f, c) => schemaClassName(c)
        case MapType(v) => s"Map[String,${scalaTypeMappingTable(v)}]"
        case _ => f.descriptionString
      }
    }
    val fields = schema.fields.toList
    // build the list of field names (_0, ..., _n)
    val fieldStr = fields.zipWithIndex.map{ case (f, i) =>
           s"_$i : ${typeName(f.fType, f.name)}"}.mkString(", ")

    // construct the mkString method
    //   we have to handle the different types here:
    //      TupleType -> ()
    //      BagType -> {}
    val toStr = fields.zipWithIndex.map{
      case (f, i) => f.fType match {
        case BagType(_) => s""""{" + _$i.mkString(",") + "}""""
        case _ => s"_$i"
      }
    }.mkString(" + _c + ")

    callST("schema_class", Map("name" -> schemaClassName(schema.className),
                              "fields" -> fieldStr,
                              "string_rep" -> toStr))
  }

  /**
   * Generate code for the given Pig operator.
   *
   * @param node the operator (an instance of PigOperator)
   * @return a string representing the code
   */
  def emitNode(node: PigOperator): String = {
    node.checkPipeNames
    node match {
        /*
         * NOTE: Don't use "out" here -> it refers only to initial constructor argument but isn't consistent
         *       after changing the pipe name. Instead, use node.outPipeName
         */
      case Load(out, file, schema, func, params) => emitLoad(node, file, func, params)
      case Dump(in) => callST("dump", Map("in"->node.inPipeName))
      case Store(in, file, func, params) => emitStore(node, file, func, params)
      case Describe(in) => s"""println("${node.schemaToString}")"""
      case SplitInto(in, splits) => callST("splitInto", Map("in"->node.inPipeName, "out"->node.outPipeNames, "pred"->splits.map(s => emitPredicate(node.schema, s.expr))))
      case Union(out, rels) => callST("union", Map("out"->node.outPipeName,"in"->node.inPipeName,"others"->node.inPipeNames.tail))
      case Sample(out, in, expr) => callST("sample", Map("out"->node.outPipeName,"in"->node.inPipeName,"expr"->emitExpr(node.schema, expr)))
      case StreamOp(out, in, op, params, schema) => callST("streamOp", Map("out"->node.outPipeName,"op"->op,"in"->node.inPipeName,"params"->emitParamList(node.schema, params)))
      // case MacroOp(out, name, params) => callST("call_macro", Map("out"->node.outPipeName,"macro_name"->name,"params"->emitMacroParamList(node.schema, params)))
      case HdfsCmd(cmd, params) => callST("fs", Map("cmd"->cmd, "params"->params))
      case RScript(out, in, script, schema) => callST("rscript", Map("out"->node.outPipeName,"in"->node.inputs.head.name,"script"->quote(script)))
      case ConstructBag(in, ref) => "" // used only inside macros
      case DefineMacroCmd(_, _, _, _) => "" // code is inlined in MacroOp; no need to generate it here again
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
