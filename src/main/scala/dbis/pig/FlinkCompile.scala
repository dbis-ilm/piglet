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
    groupingExpr.keyList.map(e => emitRef(schema, e, "", requiresTypeCast = false)).mkString(",")
  }

  def emitJoinKey(schema: Option[Schema], joinExpr: List[Ref]): String = {
    if (joinExpr.size == 1)
      emitRef(schema, joinExpr.head, "")
    else
      s"Array(${joinExpr.map(e => emitRef(schema, e, "")).mkString(",")}).mkString"
  }

  def quote(s: String): String = s"$s"

/*
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
*/
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
      case Load(out, file, schema, func, params) => { 
        val tryST = group.instanceOf("loader")
        if (tryST.isSuccess) {
          val st = tryST.get
          st.add("out", out)
          st.add("file", file)
          st.add("size", (1 to (schema.get.fields.size-1)).toList)
          if (func == "") st.render()
          else{
            val parameters = if (params != null && params.nonEmpty) params.map(quote(_)).mkString(",") else ""
            st.add("params", parameters)
            if (func == "PigStorage") st.add("pfunc", true)
            else if (func == "RDFFileStorage") st.add("rdffunc", true)
            st.render()
          }
        } 
        else throw new Exception(s"Template for node '$node' not implemented or not found")
      }
      case Dump(in) => { 
        val tryST = group.instanceOf("dump")
        if (tryST.isSuccess) {    
          val st = tryST.get
          st.add("in", node.inPipeNames.head)
          st.render()
        } 
        else throw new Exception(s"Template for node '$node' not implemented or not found")
      }
      case Store(in, file) => { 
        val tryST = group.instanceOf("store")
        if (tryST.isSuccess) {
          val st = tryST.get
          st.add("in", node.inPipeNames.head)
          st.add("file", file)
          st.render()
        }
        else throw new Exception(s"Template for node '$node' not implemented or not found")
      }
      case Describe(in) => { s"""println("${node.schemaToString}")""" }
      case Filter(out, in, pred) => { 
        val tryST = group.instanceOf("filter")
        if (tryST.isSuccess) {
          val st = tryST.get
          st.add("out", out)
          st.add("in", node.inPipeNames.head)
          st.add("pred", emitPredicate(node.schema, pred))
          st.render()
        }
        else throw new Exception(s"Template for node '$node' not implemented or not found")
      }
      case Foreach(out, in, expr) => { 
        val tryST = group.instanceOf("foreach")
        if (tryST.isSuccess) {
          val st = tryST.get
          st.add("out", out)
          st.add("in", node.inPipeNames.head)
          st.add("expr", emitGenerator(node.schema, expr))
          st.render()
        }
        else throw new Exception(s"Template for node '$node' not implemented or not found")
      }
      case Grouping(out, in, groupExpr) => {
        val tryST = group.instanceOf("groupBy")
        if (tryST.isSuccess) {
          val st = tryST.get
          st.add("out", out)
          st.add("in", node.inPipeNames.head)
          if (groupExpr.keyList.isEmpty) st.render()
          else {
            st.add("expr", emitGrouping(node.schema, groupExpr))
            st.render()
          }
        }
        else throw new Exception(s"Template for node '$node' not implemented or not found")
      }
      case Distinct(out, in) => {
        val tryST = group.instanceOf("distinct")
        if (tryST.isSuccess) {
          val st = tryST.get
          st.add("out", out)
          st.add("in", node.inPipeNames.head)
          st.render()
        }
        else throw new Exception(s"Template for node '$node' not implemented or not found")
      }
      case Limit(out, in, num) => { 
        val tryST = group.instanceOf("limit")
        if (tryST.isSuccess) {
          val st = tryST.get
          st.add("out", out)
          st.add("in", node.inPipeNames.head)
          st.add("num", num)
          st.render()
        }
        else throw new Exception(s"Template for node '$node' not implemented or not found")
      }
      case Join(out, rels, exprs) => { //TODO: Multiple Joins, Window Parameters
        val tryST = group.instanceOf("join")
        if (tryST.isSuccess) {
          val st = tryST.get
          val keys = exprs.map(k => emitJoinKey(node.schema, k))
          val res = rels.zip(keys)
          val lsize = (2 to node.inputs.head.producer.schema.get.fields.size).toList
          val rsize = (1 to node.inputs.tail.head.producer.schema.get.fields.size).toList
          st.add("lsize", lsize)
          st.add("rsize", rsize)
          st.add("out", out)
          st.add("rel1", res.head._1)
          st.add("key1", res.head._2)
          st.add("rel2", res.tail.head._1)
          st.add("key2", res.tail.head._2)
          st.render()
        }
        else throw new Exception(s"Template for node '$node' not implemented or not found")
    }
    case Union(out, rels) => {
      val tryST = group.instanceOf("union")
      if (tryST.isSuccess) {
        val st = tryST.get
        st.add("out", out)
        st.add("in", rels.head)
        st.add("others", rels.tail.toList.mkString(","))
        st.render()
      }
      else throw new Exception(s"Template for node '$node' not implemented or not found")
    }
    case Sample(out, in, expr) => { s"val $out = ${node.inPipeNames.head}"}//TODO
    case OrderBy(out, in, orderSpec) => { s"val $out = ${node.inPipeNames.head}"} // TODO
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
    case _ => { throw new Exception(s"Template for node '$node' not implemented or not found") }
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
