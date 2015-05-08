package dbis.pig

/**
 * Created by Philipp on 07.05.15.
 */

import org.clapper.scalasti._

//case class UDF(name: String, numParams: Int, isAggregate: Boolean)

class FlinkGenCode extends GenCodeBase {
  val templateFile = "src/main/resources/flink-template.stg"
  //TODO: use Flink aggregate
  val funcTable = Map("COUNT" -> UDF("PigFuncs.count", 1, true),
                      "AVG" -> UDF("PigFuncs.average", 1, true),
                      "SUM" -> UDF("PigFuncs.sum", 1, true),
                      "MIN" -> UDF("PigFuncs.min", 1, true),
                      "MAX" -> UDF("PigFuncs.max", 1, true),
                      "TOKENIZE" -> UDF("PigFuncs.tokenize", 1, false),
                      "TOMAP" -> UDF("PigFuncs.toMap", Int.MaxValue, false)
  )

  // TODO: complex types
  val typeTable = Map[PigType, String](Types.IntType -> "toInt",
                      Types.LongType -> "toLong",
                      Types.FloatType -> "toFloat",
                      Types.DoubleType -> "toDouble",
                      Types.CharArrayType -> "toString")

  def emitRef(schema: Option[Schema], ref: Ref, tuplePrefix: String = "t", requiresTypeCast: Boolean = true): String = ref match {
    case NamedField(f) => schema match {
      case Some(s) => {
        val idx = s.indexOfField(f)
        require(idx >= 0) // the field doesn't exist in the schema: shouldn't occur because it was checked before
        if (requiresTypeCast) {
          val field = s.field(idx)
          val typeCast = typeTable(field.fType)
          s"${tuplePrefix}(${idx}).${typeCast}"
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

  def quote(s: String): String = s"$s"

  def emitLoader(file: String, loaderFunc: String, loaderParams: List[String], group: STGroup): String = {
    val tryST = group.instanceOf("loader")
    if (tryST.isSuccess) {
      val st = tryST.get
      st.add("file", file)
      if (loaderFunc == ""){
        st.render()
      }  
      else{
        val params = if (loaderParams != null && loaderParams.nonEmpty) ", " + loaderParams.map(quote(_)).mkString(",") else ""
        st.add("func", loaderFunc)
        st.add("params", params)
        st.render()
      }
    } 
    else ""
  }

  def emitExpr(schema: Option[Schema], expr: ArithmeticExpr): String = expr match {
    case CastExpr(t, e) => {
      // TODO: check for invalid type
      val targetType = typeTable(t)
      s"${emitExpr(schema, e)}.$targetType"
    }
    case MSign(e) => s"-${emitExpr(schema, e)}"
    case Add(e1, e2) => s"${emitExpr(schema, e1)} + ${emitExpr(schema, e2)}"
    case Minus(e1, e2) => s"${emitExpr(schema, e1)} - ${emitExpr(schema, e2)}"
    case Mult(e1, e2) => s"${emitExpr(schema, e1)} * ${emitExpr(schema, e2)}"
    case Div(e1, e2) => s"${emitExpr(schema, e1)} / ${emitExpr(schema, e2)}"
    case RefExpr(e) => s"${emitRef(schema, e)}"
    case Func(f, params) => {
      val udf = funcTable(f)
      // TODO: check whether f exists + size of params
      if (udf.isAggregate)
        s"${udf.name}(${emitExpr(schema, params.head)}.asInstanceOf[Seq[Any]])"
      else
        s"${udf.name}(${params.map(e => emitExpr(schema, e)).mkString(",")})"
    }
    case _ => ""
  }

  def emitGenerator(schema: Option[Schema], genExprs: List[GeneratorExpr]): String = {
    s"List(${genExprs.map(e => emitExpr(schema, e.expr)).mkString(",")})"
  }

  def emitNode(node: PigOperator): String = {
    val group = STGroupFile(templateFile)
    node match {
      case Load(out, file, schema, func, params) => { s"""val $out = ${emitLoader(file, func, params, group)}""" }
      case Dump(in) => { s"""${node.inPipeNames.head}.print()""" }
      case Store(in, file) => { s"""${node.inPipeNames.head}.writeAsText("${file}")""" }
      case Describe(in) => { s"""println("${node.schemaToString}")""" }
      case Filter(out, in, pred) => { s"val $out = ${node.inPipeNames.head}.filter(t => {${emitPredicate(node.schema, pred)}})" }
      case Foreach(out, in, expr) => { s"val $out = ${node.inPipeNames.head}.map(t => ${emitGenerator(node.schema, expr)})" }

//    case Grouping(out, in, groupExpr) => {
//      if (groupExpr.keyList.isEmpty) s"val $out = ${node.inPipeNames.head}.glom"
//      else s"val $out = ${node.inPipeNames.head}.groupBy(t => {${emitGrouping(node.schema, groupExpr)}}).map{case (k,v) => List(k,v)}" }
//    case Distinct(out, in) => { s"val $out = ${node.inPipeNames.head}.distinct" }
//    case Limit(out, in, num) => { s"val $out = sc.parallelize(${node.inPipeNames.head}.take($num))" }
      case Join(out, rels, exprs) => {
        val res = rels.zip(exprs)
        val s1 = res.map{case (rel, expr) => s"val ${rel}_k = ${rel}.map(t => {${emitJoinKey(node.schema, expr)}})\n"}.mkString
        s1 + s"val $out = ${rels.head}" + rels.tail.map{other => s".join(${other}).onWindow(5, TimeUnit.SECONDS).where(${rels.head}_k).equalTo(${other}_k)"}.mkString
      }
      case Union(out, rels) => { s"val $out = ${rels.head}.merge(" + rels.tail.map{other => s"${other}"}.mkString(",") + ")" }
//    case Sample(out, in, expr) => { s"val $out = ${node.inPipeNames.head}.sample(${emitExpr(node.schema, expr)})"}
//    case OrderBy(out, in, orderSpec) => { s"val $out = ${node.inPipeNames.head}.sortBy()"} // TODO
      case _ => { "" }
    }
  }

  def emitHeader(scriptName: String): String = {
    var header = ""
    val group = STGroupFile(templateFile)
    var tryST = group.instanceOf("init_code")
    if (tryST.isSuccess) {    
       header+=tryST.get.render()
    }
    tryST = group.instanceOf("begin_query")
    if (tryST.isSuccess){
      val st = tryST.get
      st.add("name", scriptName)
      header+=st.render()
    }
    header
  }

  def emitFooter: String = {
    var footer = ""
    val group = STGroupFile(templateFile)
    val tryST = group.instanceOf("end_query")
    if (tryST.isSuccess) {
      val st = tryST.get
      st.add("name", "Starting Flink Query")
      footer+=st.render()
    }
    footer
  }
}

class FlinkCompile extends Compile {
  override val codeGen = new FlinkGenCode
}
