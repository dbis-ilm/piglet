package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen._
import dbis.piglet.expr._
import dbis.piglet.op.{GroupingExpression, PigOperator}
import dbis.piglet.schema._
import dbis.piglet.udf.UDFTable
import org.clapper.scalasti.ST


object ScalaEmitter {
  // TODO: complex types
  val scalaTypeMappingTable = Map[PigType, String](
    Types.BooleanType -> "Boolean",
    Types.IntType -> "Int",
    Types.LongType -> "Long",
    Types.FloatType -> "Float",
    Types.DoubleType -> "Double",
    Types.CharArrayType -> "String",
    Types.ByteArrayType -> "String", //TODO: check this - maybe this should be Any
    Types.AnyType -> "String") //TODO: check this

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
    * @param name
    * @return
    */
  def schemaClassName(name: String) = s"_${name}_Tuple"

  /**
    * Generate Scala code for a reference to a named field, a positional field or a tuple/map derefence.
    *
    * @param ctx an object representing context information for code generation
    * @param ref the reference object
    * @return the generated code
    */
  def emitRef(ctx: CodeGenContext, ref: Ref): String = ref match {

    case nf @ NamedField(f, _) => 
      if (ctx.asBoolean("namedRef")) {
        // check if f exists in the schema
        ctx.schema match {
          case Some(s) => {
            val p = s.indexOfField(nf)
            if (p != -1)
              s"${ctx.asString("tuplePrefix")}._$p"
            else
              f // TODO: check whether thus is a valid field (or did we check it already in checkSchemaConformance??)
          }
          case None =>
            // if we don't have a schema this is not allowed
            throw new CodeGenException(s"invalid field name $f (named ref not found)")
        }
      }
      else {
        val pos = ctx.schema.get.indexOfField(nf)
        if (pos == -1) {
        	println(s"ctx schema ${ctx.schema.get}")
          throw new CodeGenException(s"invalid field name $nf (field position not found)")
        }
        s"${ctx.asString("tuplePrefix")}._$pos" // s"$tuplePrefix.$f"
      }
    case PositionalField(pos) => ctx.schema match {
      case Some(s) => s"${ctx.asString("tuplePrefix")}._$pos"
      case None =>
        // if we don't have a schema the Record class is used
        s"${ctx.asString("tuplePrefix")}.get($pos)"
    }
    case Value(v) => v.toString
    case DerefMap(m, k) => s"${emitRef(ctx, m)}(${k})"
    case DerefTuple(r1, r2) =>
      if (ctx.asBoolean("aggregate"))
        s"${emitRef(CodeGenContext(ctx, Map("tuplePrefix" -> "t")), r1)}.map(e => e${emitRef(CodeGenContext(ctx, Map("schema" -> tupleSchema(ctx.schema, r1), "tuplePrefix" -> "")), r2)})"
      else {/*
        ctx.events match {
          case Some(evs) => {
            // we try to find r1 in the specification of the events and retrieve the position (or -1 if not found)
            val res = evs.complex.zipWithIndex.filter{ case (e, pos)  => e.simplePattern.asInstanceOf[SimplePattern].name == r1.toString  }
            if (res.length != 1)
              s"${emitRef(CodeGenContext(schema = ctx.schema, tuplePrefix = "t", events = ctx.events), r1)}${emitRef(CodeGenContext(schema = tupleSchema(ctx.schema, r1), tuplePrefix = "", aggregate = ctx.aggregate, namedRef = ctx.namedRef, events = ctx.events), r2)}"
            else {
              val p = r2 match {
                case nf @ NamedField(f, _) => ctx.schema.get.indexOfField(nf)
                case PositionalField (pos) => pos
                case _ => 0
              }
              if (p == -1)
                throw new CodeGenException(s"invalid field name $r2 in event ${r1.toString}")

              s"rvalues.events(${res(0)._2})._$p)"  //TODO: work more on other related values
            }
          }
          case None =>
          */
          s"${emitRef(CodeGenContext(ctx, Map("tuplePrefix" -> "t")), r1)}${emitRef(CodeGenContext(ctx, Map("schema" -> tupleSchema(ctx.schema, r1), "tuplePrefix" -> "")), r2)}"

        }

    case _ => { "" }
  }


  /**
    * Generate Scala code for a predicate on expressions used e.g. in a MATCHER or SPLIT INTO statement.
    *
    * @param ctx an object representing additional information about the context
    * @param predicate the actual predicate
    * @return a string representation of the generated Scala code
    */
  def emitPredicate(ctx: CodeGenContext, predicate: Predicate): String =  predicate match {
    case Eq(left, right) => { s"${emitExpr(ctx, left)} == ${emitExpr(ctx, right)}"}
    case Neq(left, right) => { s"${emitExpr(ctx, left)} != ${emitExpr(ctx, right)}"}
    case Leq(left, right) => { s"${emitExpr(ctx, left)} <= ${emitExpr(ctx, right)}"}
    case Lt(left, right) => { s"${emitExpr(ctx, left)} < ${emitExpr(ctx, right)}"}
    case Geq(left, right) => { s"${emitExpr(ctx, left)} >= ${emitExpr(ctx, right)}"}
    case Gt(left, right) => { s"${emitExpr(ctx, left)} > ${emitExpr(ctx, right)}"}
    case And(left, right) => s"${emitPredicate(ctx, left)} && ${emitPredicate(ctx, right)}"
    case Or(left, right) => s"${emitPredicate(ctx, left)} || ${emitPredicate(ctx, right)}"
    case Not(pred) => s"!(${emitPredicate(ctx, pred)})"
    case PPredicate(pred) => s"(${emitPredicate(ctx, pred)})"
    case _ => throw new CodeGenException(s"unknown predicate: $predicate")
  }

  /**
    * Generate Scala code for implementing a Pig expression.
    *
    * @param ctx an object representing additional information about the context
    * @param expr the actual expression
    * @return the the generated code representing the expression
    */
  def emitExpr(ctx: CodeGenContext,
               expr: ArithmeticExpr): String = expr match {
    case CastExpr(t, e) => {
      // TODO: check for invalid type
      val targetType = scalaTypeMappingTable(t)
      s"${emitExpr(ctx, e)}.to$targetType"
    }
    case PExpr(e) => s"(${emitExpr(ctx, e)})"
    case MSign(e) => s"-${emitExpr(ctx, e)}"
    case Add(e1, e2) => s"${emitExpr(ctx, e1)} + ${emitExpr(ctx, e2)}"
    case Minus(e1, e2) => s"${emitExpr(ctx, e1)} - ${emitExpr(ctx, e2)}"
    case Mult(e1, e2) => s"${emitExpr(ctx, e1)} * ${emitExpr(ctx, e2)}"
    case Div(e1, e2) => s"${emitExpr(ctx, e1)} / ${emitExpr(ctx, e2)}"
    case RefExpr(e) => s"${emitRef(CodeGenContext(ctx, Map[String, Any]("tuplePrefix" -> "t")), e)}"
    case Func(f, params) => emitFuncCall(ctx, f, params)
    case FlattenExpr(e) => flattenExpr(ctx, e)
    case ConstructTupleExpr(exprs) => {
      val exType = expr.resultType(ctx.schema).asInstanceOf[TupleType]
      val s = Schema(new BagType(exType))
      s"${schemaClassName(s.className)}(${exprs.map(e => emitExpr(ctx, e)).mkString(",")})"
    }
    case ConstructBagExpr(exprs) => {
      val exType = expr.resultType(ctx.schema).asInstanceOf[BagType]
      val s = Schema(exType)
      s"List(${exprs.map(e => s"${schemaClassName(s.className)}(${emitExpr(ctx, e)})").mkString(",")})"
    }
    case ConstructMapExpr(exprs) => {
      val exType = expr.resultType(ctx.schema).asInstanceOf[MapType]
      val valType = exType.valueType
      val exprList = exprs.map(e => emitExpr(ctx, e))
      // convert the list (e1, e2, e3, e4) into a list of (e1 -> e2, e3 -> e4)
      val mapStr = exprList.zip(exprList.tail).zipWithIndex.filter{
        case (p, i) => i % 2 == 0
      }.map{case (p, i) => s"${p._1} -> ${p._2}"}.mkString(",")
      s"Map[String,${scalaTypeMappingTable(valType)}](${mapStr})"
    }
    case ConstructMatrixExpr(ty, rows, cols, expr) => {
      val mType = if (ty.charAt(1) == 'i') "Int" else "Double"
      s"new DenseMatrix[$mType]($rows, $cols, ${emitExpr(ctx, expr)}.map(v => v._0).toArray)"
    }

    case ConstructGeometryExpr(expr,time) => {
      val timeStr = time.map { t =>
        t match {
          case Instant(value) => s"Instant(${emitExpr(ctx, value)})"
          case Interval(s, Some(e)) => s"Interval(${emitExpr(ctx, s)}, ${emitExpr(ctx, e)})"
          case Interval(s, None) => s"Interval(${emitExpr(ctx, s)}, None)"
          case _ => throw new CodeGenException(s"Unsupported temporal expression type $t")
        }
      }
      s"STObject(new WKTReader().read(${emitExpr(ctx, expr)}) ${ if(timeStr.isDefined) s", $timeStr.get" else ""  } )"
    }

    case _ => throw new CodeGenException(s"unsupported expression: $expr")
  }

  /**
    * Generate Scala code for a function call with parameters.
    *
    * @param ctx an object representing context information for code generation
    * @param f the function name
    * @param params the list of parameters
    * @return the generated Scala code
    */
  def emitFuncCall(ctx: CodeGenContext, f: String, params: List[ArithmeticExpr]): String = {
    val pTypes = params.map(p => p.resultType(ctx.schema))
    UDFTable.findUDF(f, pTypes) match {
      case Some(udf) => {
        // println(s"udf: $f found: " + udf)
        if (udf.isAggregate) {
          s"${udf.scalaName}(${emitExpr(CodeGenContext(ctx, Map[String, Any]("aggregate" -> true)), params.head)})"
        }
        else {
          val mapStr = if (udf.resultType.isInstanceOf[ComplexType]) {
            udf.resultType match {
              case BagType(v) => s".map(${schemaClassName(v.className)}(_))"
              case _ => "" // TODO: handle TupleType and MapType
            }
          } else ""
          val paramExprList = params.zipWithIndex.map { case (e, i) =>
            // if we know the expected parameter type and the expression type
            // is a generic bytearray then we cast it to the expected type
            val typeCast = if (udf.paramTypes.length > i && // make sure the function has enough parameters
              e.resultType(ctx.schema) == Types.ByteArrayType &&
              (udf.paramTypes(i) != Types.ByteArrayType && udf.paramTypes(i) != Types.AnyType)) {
              s".asInstanceOf[${scalaTypeMappingTable(udf.paramTypes(i))}]"
            } else ""
            emitExpr(ctx, e) + typeCast
          }

          s"${udf.scalaName}(${paramExprList.mkString(",")})${mapStr}"
        }
      }
      case None => {
        // println(s"udf: $f not found")
        // check if we have have an alias in DataflowPlan
        if (ctx.udfAliases.nonEmpty && ctx.udfAliases.get.contains(f)) {
          val alias = ctx.udfAliases.get(f)
          val paramList = alias._2 ::: params.map(e => emitExpr(ctx, e))
          s"${alias._1}(${paramList.mkString(",")})"
        }
        else {
          // we don't know the function yet, let's assume there is a corresponding Scala function
          s"$f(${params.map(e => emitExpr(ctx, e)).mkString(",")})"
        }
      }
    }
  }
  
  /**
    * Construct the extract function for source operators.
    *
    * @param node the PigOperator for loading data
    * @param loaderFunc the loader function
    * @return a parameter map with class and extractor elements
    */
  def emitExtractorFunc(node: PigOperator, loaderFunc: Option[String]): Map[String, Any] = {
    def schemaExtractor(schema: Schema): String =
      schema.fields.zipWithIndex.map{case (f, i) =>
        // we cannot perform a "toAny" - therefore, we treat bytearray as String here
        val t = ScalaEmitter.scalaTypeMappingTable(f.fType); s"data($i).to${if (t == "Any") "String" else t}"
      }.mkString(", ")

    def jdbcSchemaExtractor(schema: Schema): String =
      schema.fields.zipWithIndex.map{case (f, i) => s"data.get${ScalaEmitter.scalaTypeMappingTable(f.fType)}($i)"}.mkString(", ")

    var paramMap = Map[String, Any]()
    node.schema match {
      case Some(s) => if (loaderFunc.nonEmpty && loaderFunc.get == "JdbcStorage")
      // JdbcStorage provides already types results, therefore we need an extractor which calls
      // only the appropriate get functions on sql.Row
        paramMap += ("extractor" ->
          s"""(data: org.apache.spark.sql.Row) => ${ScalaEmitter.schemaClassName(s.className)}(${jdbcSchemaExtractor(s)})""",
          "class" -> ScalaEmitter.schemaClassName(s.className))
      else
        paramMap += ("extractor" ->
          s"""(data: Array[String]) => ${ScalaEmitter.schemaClassName(s.className)}(${schemaExtractor(s)})""",
          "class" -> ScalaEmitter.schemaClassName(s.className))
      case None => {
        paramMap += ("extractor" -> "(data: Array[String]) => Record(data)", "class" -> "Record")
      }
    }
    paramMap
  }
  
  /**
    * Generates Scala code for a grouping expression in GROUP BY. We construct code for map
    * in the form "map(t => {(t(0),t(1),...)}" if t(0), t(1) are grouping attributes.
    *
    * @param ctx an object representing context information for code generation
    * @param groupingExpr the actual grouping expression object
    * @return a string representation of the generated Scala code
    */
  def emitGroupExpr(ctx: CodeGenContext, groupingExpr: GroupingExpression): String = {
    if (groupingExpr.keyList.size == 1)
      groupingExpr.keyList.map(e => emitRef(ctx, e)).mkString
    else
      "(" + groupingExpr.keyList.map(e => emitRef(ctx, e)).mkString(",") + ")"
  }

  /**
    * Constructs the Scala code for flattening a tuple. We have to determine the field in the
    * input schema refering to a tuple type and extract all fields of this tuple type.
    *
    * @param ctx an object representing context information for code generation
    * @param e the expression to be flattened (should be a RefExpr)
    * @return a string representation of the Scala code
    */
  def flattenExpr(ctx: CodeGenContext, e: ArithmeticExpr): String = {
    if (ctx.schema.isEmpty) throw new CodeGenException("cannot flatten a tuple without a schema")
    // we need the field name used in Scala (either the actual name or _<position>) as well as
    // the actual field
    val (refName, field) = e match {
      case RefExpr(r) => r match {
        case nf@NamedField(n, _) => ("_" + ctx.schema.get.indexOfField(nf), ctx.schema.get.field(nf))
        case PositionalField(p) => ("_" + p.toString, ctx.schema.get.field(p))
        // either a named or a positional field: all other cases are not allowed!?
        case _ => throw new CodeGenException("invalid flatten expression: argument isn't a reference")
      }
      case _ => throw new CodeGenException(s"invalid flatten expression: argument $e isn't a reference")
    }
    if (field.fType.tc == TypeCode.TupleType) {
      // we flatten a tuple
      val tupleType = field.fType.asInstanceOf[TupleType]
      // finally, produce a list of t.<refName>.<fieldPos>
      tupleType.fields.zipWithIndex.map { case (f, i) => s"t.${refName}._$i" }.mkString(", ")
    }
    else if (field.fType.tc == TypeCode.BagType) {
      // we flatten a bag
      val bagType = field.fType.asInstanceOf[BagType]
      s"t.${refName}"
    }
    else
    // other types than tuple and bag cannot be flattened
      throw new CodeGenException("invalid flatten expression: argument doesn't refer to a tuple or bag")
  }

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
    * Generate code for a class representing a schema type.
    *
    * @param values
    * @return
    */
  def emitSchemaClass(values: (String, String, String, String, String)): (String, String) = {
    val (name, fieldNames, fieldTypes, fieldStr, toStr) = values

    val code = CodeEmitter.render("""  case class <name> (<fields>) extends java.io.Serializable with SchemaClass {
                                    |    override def mkString(_c: String = ",") = <string_rep>
                                    |  }
                                    |""".stripMargin, Map("name" -> name,
      "fieldNames" -> fieldNames,
      "fieldTypes" -> fieldTypes,
      "fields"   -> fieldStr,
      "string_rep" -> toStr))

    (name, code)
  }

  def emitSchemaConverters(values: (String, String, String, String, String)): String = {
    val (name, fieldNames, fieldTypes, _, _) = values

    CodeEmitter.render("""<if (fieldNames)>
                         |  implicit def convert<name>(t: (<fieldTypes>)): <name> = <name>(<fieldNames>)
                         |<endif>""".stripMargin, Map("name" -> name,
      "fieldNames" -> fieldNames,
      "fieldTypes" -> fieldTypes
    ))
  }

  def createSchemaInfo(schema: Schema) = {
    def typeName(f: PigType, n: String) = scalaTypeMappingTable.get(f) match {
      case Some(n) => n
      case None => f match {
        // if we have a bag without a name then we assume that we have got
        // a case class with _<field_name>_Tuple
        case BagType(v) => s"Iterable[_${v.className}_Tuple]"
        case TupleType(f, c) => schemaClassName(c)
        case MapType(v) => s"Map[String,${scalaTypeMappingTable(v)}]"
        case MatrixType(v, rows, cols, rep) => s"DenseMatrix[${if (v.tc == TypeCode.IntType) "Int" else "Double"}]"
        case _ => f.descriptionString
      }
    }
    val fieldList = schema.fields.toList
    // build the list of field names (_0, ..., _n)
    val fieldNames = if (fieldList.size==1) "t" else fieldList.indices.map(t => "t._"+(t+1)).mkString(", ")
    val fieldTypes = fieldList.map(f => s"${typeName(f.fType, f.name)}").mkString(", ")
    val fields = fieldList.zipWithIndex.map{ case (f, i) =>
      (s"_$i", s"${typeName(f.fType, f.name)}")}
    val fieldStr = fields.map(t => t._1 + ": " + t._2).mkString(", ")

    // construct the mkString method
    //   we have to handle the different types here:
    //      TupleType -> ()
    //      BagType -> {}
    val toStr = fieldList.zipWithIndex.map{
      case (f, i) => f.fType match {
        case BagType(_) => s""""{" + _$i.mkString(",") + "}""""
        case MapType(v) => s""""[" + _$i.map{ case (k,v) => s"$$k#$$v" }.mkString(",") + "]""""
        case _ => s"_$i" + (if (f.fType.tc != TypeCode.CharArrayType && fields.length == 1) ".toString" else "")
      }
    }.mkString(" + _c + ")

    val name = schemaClassName(schema.className)

    (name, fieldNames, fieldTypes, fieldStr, toStr)
  }

  /**
    *
    * @param ctx an object representing context information for code generation
    * @param params the list of parameters (as Refs)
    * @return the generated code
    */
  def emitParamList(ctx: CodeGenContext, params: Option[List[Ref]]): String = params match {
    case Some(refList) => if (refList.nonEmpty) s",${refList.map(r => emitRef(ctx, r)).mkString(",")}" else ""
    case None => ""
  }

}