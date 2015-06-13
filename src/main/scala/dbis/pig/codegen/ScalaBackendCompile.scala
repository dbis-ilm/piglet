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
import dbis.pig.schema.{PigType, Schema, SchemaException, Types}
import org.clapper.scalasti._

class ScalaBackendGenCode(templateFile: String) extends GenCodeBase {

  case class UDF(name: String, numParams: Int, isAggregate: Boolean)
  case class TemplateException(msg: String) extends Exception(msg)
  
//  val templateFile = "src/main/resources/flink-template.stg"

  val funcTable = Map("COUNT" -> UDF("PigFuncs.count", 1, true),
                      "AVG" -> UDF("PigFuncs.average", 1, true),
                      "SUM" -> UDF("PigFuncs.sum", 1, true),
                      "MIN" -> UDF("PigFuncs.min", 1, true),
                      "MAX" -> UDF("PigFuncs.max", 1, true),
                      "TOKENIZE" -> UDF("PigFuncs.tokenize", 1, false),
                      "TOMAP" -> UDF("PigFuncs.toMap", Int.MaxValue, false)
  )

  // TODO: complex types
  val scalaTypeMappingTable = Map[PigType, String](Types.IntType -> "Int",
    Types.LongType -> "Long",
    Types.FloatType -> "Float",
    Types.DoubleType -> "Double",
    Types.CharArrayType -> "String")

  def emitRef(schema: Option[Schema], ref: Ref, tuplePrefix: String = "t", requiresTypeCast: Boolean = true): String = ref match {
    case NamedField(f) => schema match {
      case Some(s) => {
        val idx = s.indexOfField(f)
        if (idx == -1) println("----------> " + f)
        require(idx >= 0) // the field doesn't exist in the schema: shouldn't occur because it was checked before
        if (requiresTypeCast) {
          val field = s.field(idx)
          val typeCast = scalaTypeMappingTable(field.fType)
          s"${tuplePrefix}(${idx}).to${typeCast}"
        }
        else
          s"${tuplePrefix}(${idx})"
      }
      case None => throw new SchemaException(s"unknown schema for field $f")
    } // TODO: should be position of field
    case PositionalField(pos) => s"$tuplePrefix($pos)"
    case Value(v) => v.toString
    case DerefTuple(r1, r2) => s"${emitRef(schema, r1)}.asInstanceOf[List[Any]]${emitRef(schema, r2, "")}"
    case DerefMap(m, k) => s"${emitRef(schema, m)}.asInstanceOf[Map[String,Any]](${k})"
    case _ => { "" }
  }

  def emitPredicate(schema: Option[Schema], predicate: Predicate): String = predicate match {
    case Eq(left, right) => { s"${emitExpr(schema, left)} == ${emitExpr(schema, right)}"}
    case Neq(left, right) => { s"${emitExpr(schema, left)} != ${emitExpr(schema, right)}"}
    case Leq(left, right) => { s"${emitExpr(schema, left)} <= ${emitExpr(schema, right)}"}
    case Lt(left, right) => { s"${emitExpr(schema, left)} < ${emitExpr(schema, right)}"}
    case Geq(left, right) => { s"${emitExpr(schema, left)} >= ${emitExpr(schema, right)}"}
    case Gt(left, right) => { s"${emitExpr(schema, left)} > ${emitExpr(schema, right)}"}
    case _ => { "" }
  }

  def emitGrouping(schema: Option[Schema], groupingExpr: GroupingExpression): String = {
    groupingExpr.keyList.map(e => emitRef(schema, e, requiresTypeCast = false)).mkString(",")
  }

  def emitJoinKey(schema: Option[Schema], joinExpr: List[Ref]): String = {
    if (joinExpr.size == 1)
      emitRef(schema, joinExpr.head)
    else
      s"Array(${joinExpr.map(e => emitRef(schema, e)).mkString(",")}).mkString"
  }

  def quote(s: String): String = s.replace('\'', '"')

  def emitLoader(out: String, file: String, loaderFunc: String, loaderParams: List[String]): String = {
    if (loaderFunc == "")
      callST("loader", Map("out"->out,"file"->file))
    else {
      val params = if (loaderParams != null && loaderParams.nonEmpty) ", " + loaderParams/*.map(quote(_))*/.mkString(",") else ""
      callST("loader", Map("out"->out,"file"->file,"func"->loaderFunc,"params"->params))
    }
  }

  def emitExpr(schema: Option[Schema], expr: ArithmeticExpr): String = expr match {
    case CastExpr(t, e) => {
      // TODO: check for invalid type
      val targetType = scalaTypeMappingTable(t)
      s"${emitExpr(schema, e)}.to$targetType"
    }
    case MSign(e) => s"-${emitExpr(schema, e)}"
    case Add(e1, e2) => s"${emitExpr(schema, e1)} + ${emitExpr(schema, e2)}"
    case Minus(e1, e2) => s"${emitExpr(schema, e1)} - ${emitExpr(schema, e2)}"
    case Mult(e1, e2) => s"${emitExpr(schema, e1)} * ${emitExpr(schema, e2)}"
    case Div(e1, e2) => s"${emitExpr(schema, e1)} / ${emitExpr(schema, e2)}"
    case RefExpr(e) => s"${emitRef(schema, e)}"
    case Func(f, params) => {
      if (funcTable.contains(f)) {
        val udf = funcTable(f)
        // TODO: check size of params
        if (udf.isAggregate)
          s"${udf.name}(${emitExpr(schema, params.head)}.asInstanceOf[Seq[Any]])"
        else
          s"${udf.name}(${params.map(e => emitExpr(schema, e)).mkString(",")})"
      }
      else {
        // TODO: we don't know the function yet, let's assume there is a corresponding Scala function
        s"$f(${params.map(e => emitExpr(schema, e)).mkString(",")})"
      }
    }
    case _ => ""
  }

  def emitGenerator(schema: Option[Schema], genExprs: List[GeneratorExpr]): String = {
    s"List(${genExprs.map(e => emitExpr(schema, e.expr)).mkString(",")})"
  }

  def emitParamList(schema: Option[Schema], params: Option[List[Ref]]): String = params match {
    case Some(refList) => s",${refList.map(r => emitRef(schema, r)).mkString(",")}"
    case None => ""
  }

  /**
    * Returns true if the sort order of the OrderBySpec is ascending
    *
    * @param spec
    * @return
    */
  def ascendingSortOrder(spec: OrderBySpec): Boolean = spec.dir == OrderByDirection.AscendingOrder

  def emitSortKey(schema: Option[Schema], orderSpec: List[OrderBySpec], out: String, in: String) : String = {
    if (orderSpec.size == 1)
      emitRef(schema, orderSpec.head.field)
    else
      s"custKey_${out}_${in}(${orderSpec.map(r => emitRef(schema, r.field)).mkString(",")})"
  }

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

  def emitHelperClass(node: PigOperator): String = {
    def genCmpExpr(col: Int, num: Int) : String =
      if (col == num) s"{ this.c$col compare that.c$col }"
      else s"{ if (this.c$col == that.c$col) ${genCmpExpr(col+1, num)} else this.c$col compare that.c$col }"

    node match {
      case OrderBy(out, in, orderSpec) => {
        val cname = s"custKey_${out}_${in}"
        var col = 0
        val fields = orderSpec.map(o => { col += 1; s"c$col: ${scalaTypeOfField(o.field, node.schema)}" }).mkString(", ")
        val cmpExpr = genCmpExpr(1, orderSpec.size)
        s"""
        |case class ${cname}(${fields}) extends Ordered[${cname}] {
          |  def compare(that: ${cname}) = $cmpExpr
          |}""".stripMargin
      }
      case _ => ""
    }
  }

  def listToTuple(schema: Option[Schema]): String = schema match {
    case Some(s) => '"' + (0 to s.fields.length-1).toList.map(i => s"%s").mkString(",") + '"' +
    ".format(" + (0 to s.fields.length-1).toList.map(i => s"t($i)").mkString(",") + ")" 
    case None => s"t(0)"
  }

  def emitNode(node: PigOperator): String = {
    val group = STGroupFile(templateFile)
    node match {
      case Load(out, file, schema, func, params) => emitLoader(out, file, func, params)
      case Dump(in) => callST("dump", Map("in"->in.head)) 
      case Store(in, file) => callST("store", Map("in"->in.head,"file"->file,"schema"->listToTuple(node.schema)))
      case Describe(in) => s"""println("${node.schemaToString}")"""
      case Filter(out, in, pred) => callST("filter", Map("out"->out,"in"->in.head,"pred"->emitPredicate(node.schema, pred)))
      case Foreach(out, in, gen) => {
        gen match {
          case GeneratorList(expr) => callST("foreach", Map("out" -> out, "in" -> in.head, "expr" -> emitGenerator(node.inputSchema, expr)))
          case GeneratorPlan(plan) => "" // TODO: code generator
        }
      }
      case Grouping(out, in, groupExpr) => {
        if (groupExpr.keyList.isEmpty) callST("groupBy", Map("out"->out,"in"->in.head))
        else callST("groupBy", Map("out"->out,"in"->in.head,"expr"->emitGrouping(node.inputSchema, groupExpr)))
      }
      case Distinct(out, in) => callST("distinct", Map("out"->out,"in"->in.head))
      case Limit(out, in, num) => callST("limit", Map("out"->out,"in"->in.head,"num"->num))
      case Join(out, rels, exprs) => { //TODO: Window Parameters
        val res = node.inputs.zip(exprs)
        val keys = res.map{case (i,k) => emitJoinKey(i.producer.schema, k)}
        callST("join", Map("out"->out,"rel1"->rels.head,"key1"->keys.head,"rel2"->rels.tail,"key2"->keys.tail)) 
      }
      case Union(out, rels) => callST("union", Map("out"->out,"in"->rels.head,"others"->rels.tail)) 
      case Sample(out, in, expr) => callST("sample", Map("out"->out,"in"->in.head,"expr"->emitExpr(node.schema, expr)))
      case OrderBy(out, in, orderSpec) => callST("orderBy", Map("out"->out,"in"->in.head,
        "key"->emitSortKey(node.schema, orderSpec, out, in),"asc"->ascendingSortOrder(orderSpec.head)))
      case StreamOp(out, in, op, params, schema) => callST("streamOp", Map("out"->out,"op"->op,"in"->in,
        "params"->emitParamList(node.schema, params)))
      /*     
       case Cross(out, rels) =>{ s"val $out = ${rels.head}" + rels.tail.map{other => s".cross(${other}).onWindow(5, TimeUnit.SECONDS)"}.mkString }
       case Split(out, rels, expr) => {  //TODO: emitExpr depends on how pig++ will call this OP
       val s1 = s"""
       |val split = ${rels.head}.split(${emitExpr(node.schema, expr)} match {
           |  case true => List("1st")
           |  case false => List("2nd")
           |})
         """.stripMargin
         val s2 = rels.tail.map{rel => s"""val ${rel} = split.select("1st")"""}.mkString
         s1 + s2
       }
       */
      case _ => throw new TemplateException(s"Template for node '$node' not implemented or not found")
    }
  }

  def emitImport: String = callST("init_code")

  def emitHeader(scriptName: String): String = callST("begin_query", Map("name" -> scriptName))

  def emitFooter: String = callST("end_query", Map("name" -> "Starting Query"))

  def callST(template: String): String = callST(template, Map[String,Any]())
  def callST(template:String, attributes: Map[String, Any]): String = {
    val group = STGroupFile(templateFile)
    val tryST = group.instanceOf(template)
    if (tryST.isSuccess) {
      val st = tryST.get
      if (!attributes.isEmpty){
        attributes.foreach {
          attr => st.add(attr._1, attr._2)
        }
      }
      st.render()
    }
    else throw new TemplateException(s"Template '$template' not implemented or not found") 
  }
  
}

class ScalaBackendCompile(templateFile: String) extends Compile {
  override val codeGen = new ScalaBackendGenCode(templateFile)
}
