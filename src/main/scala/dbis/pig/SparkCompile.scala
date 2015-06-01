package dbis.pig

/**
 * Created by kai on 10.04.15.
 */

case class UDF(name: String, numParams: Int, isAggregate: Boolean)

class SparkGenCode extends GenCodeBase {
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

  def emitLoader(file: String, loaderFunc: String, loaderParams: List[String]): String = {
    if (loaderFunc == "")
      s"""sc.textFile("$file").map(s => List(s))"""
    else {
      val params = if (loaderParams != null && loaderParams.nonEmpty) ", " + loaderParams.map(quote(_)).mkString(",") else ""
      s"""${loaderFunc}().load(sc, "${file}"${params})"""
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

  def emitNode(node: PigOperator): String = node match {
    case Load(out, file, schema, func, params) => { s"val $out = ${emitLoader(file, func, params)}" }
    case Dump(in) => { s"""${node.inPipeName}.collect.map(t => println(t.mkString(",")))""" }
    case Store(in, file) => { s"""${node.inPipeName}.map(t => ${listToTuple(node.schema)}).coalesce(1, true).saveAsTextFile("${file}")""" }
    case Describe(in) => { s"""println("${node.schemaToString}")""" }
    case Filter(out, in, pred) => { s"val $out = ${node.inPipeName}.filter(t => {${emitPredicate(node.schema, pred)}})" }
    case Foreach(out, in, expr) => { s"val $out = ${node.inPipeName}.map(t => ${emitGenerator(node.inputSchema, expr)})" }
    case Grouping(out, in, groupExpr) => {
      if (groupExpr.keyList.isEmpty) s"val $out = ${node.inPipeName}.glom"
      else s"val $out = ${node.inPipeName}.groupBy(t => {${emitGrouping(node.inputSchema, groupExpr)}}).map{case (k,v) => List(k,v)}" }
    case Distinct(out, in) => { s"val $out = ${node.inPipeName}.distinct" }
    case Limit(out, in, num) => { s"val $out = sc.parallelize(${node.inPipeName}.take($num))" }
    case Join(out, rels, exprs) => {
      val res = rels.zip(exprs)
      val s1 = res.map{case (rel, expr) => s"val ${rel}_kv = ${rel}.keyBy(t => {${emitJoinKey(node.schema, expr)}})\n"}.mkString
      s1 + s"val $out = ${rels.head}_kv" + rels.tail.map{other => s".join(${other}_kv)"}.mkString + ".map{case (k,v) => List(k,v)}"
    }
    case Union(out, rels) => { s"val $out = ${rels.head}" + rels.tail.map{other => s".union(${other})"}.mkString }
    case Sample(out, in, expr) => { s"val $out = ${node.inPipeName}.sample(${emitExpr(node.schema, expr)})"}
    case OrderBy(out, in, orderSpec) => {
      s"val $out = ${node.inPipeName}.keyBy(t => ${emitSortKey(node.schema, orderSpec, out, in)}).sortByKey(${ascendingSortOrder(orderSpec.head)}).map{case (k,v) => v}"}
    case StreamOp(out, in, op, params, schema) => { s"val $out = $op($in${emitParamList(node.schema, params)})"}
    case _ => { "" }
  }

  def emitImport: String = {
    s"""
       |import org.apache.spark.SparkContext
       |import org.apache.spark.SparkContext._
       |import org.apache.spark.SparkConf
       |import org.apache.spark.rdd._
       |import dbis.spark._
       |""".stripMargin
  }

  def emitHeader(scriptName: String): String = {
    s"""
       |object $scriptName {
       |    def main(args: Array[String]) {
       |      val conf = new SparkConf().setAppName("${scriptName}_App")
       |      val sc = new SparkContext(conf)
    """.stripMargin
  }

  def emitFooter: String = {
    """
      |      sc.stop()
      }
    }
    """.stripMargin
  }
}

class SparkCompile extends Compile {
  override val codeGen = new SparkGenCode
}