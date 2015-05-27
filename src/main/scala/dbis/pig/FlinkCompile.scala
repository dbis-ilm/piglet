package dbis.pig

/**
 * Created by Philipp on 07.05.15.
 */

import org.clapper.scalasti._

//case class UDF(name: String, numParams: Int, isAggregate: Boolean)
case class TemplateException(msg: String) extends Exception(msg)

class FlinkGenCode extends GenCodeBase {
  
  val templateFile = "src/main/resources/flink-template.stg"

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
        func match {
          case "PigStorage" => {
            val parameters = if (params != null && params.nonEmpty) params.map(quote(_)).mkString(",") else ""
            val iSize = schema match{
              case Some(s) => s.fields.size
              case None => throw new SchemaException(s"Could not load Schema for load operation. Make sure to define it with the AS keyword")
            }
            val iList = if(iSize>1) (1 to iSize-1).toList else List.empty[Int]
            callST("loader", Map("out"->out,"file"->file,"pfunc"->true,"params"->parameters,"schema"->iList))
          }
          case "RDFFileStorage" => callST("loader", Map("out"->out,"file"->file,"rdffunc"->true))
          case _  => callST("loader", Map("out"->out,"file"->file))
        }

      }
      case Dump(in) => callST("dump", Map("in"->in.head)) 
      case Store(in, file) => callST("store", Map("in"->in.head,"file"->file))
      case Describe(in) => { s"""println("${node.schemaToString}")""" }
      case Filter(out, in, pred) => callST("filter", Map("out"->out,"in"->in.head,"pred"->emitPredicate(node.schema, pred)))
      case Foreach(out, in, expr) => callST("foreach", Map("out"->out,"in"->in.head,"expr"->emitGenerator(node.schema, expr)))
      case Grouping(out, in, groupExpr) => {
        if (groupExpr.keyList.isEmpty) callST("groupBy", Map("out"->out,"in"->in.head))
        else callST("groupBy", Map("out"->out,"in"->in.head,"expr"->emitGrouping(node.schema, groupExpr)))
      }
      case Distinct(out, in) => callST("distinct", Map("out"->out,"in"->in.head))
      case Limit(out, in, num) => callST("limit", Map("out"->out,"in"->in.head,"num"->num))
      case Join(out, rels, exprs) => { //TODO: Multiple Joins, Window Parameters
        val keys = exprs.map(k => emitJoinKey(node.schema, k))
        val lSize = node.inputs.head.producer.schema match{
          case Some(s) => s.fields.size
          case None => throw new SchemaException(s"Could not load Schema for first join relation")
        }
        val rSize = node.inputs.tail.head.producer.schema match{
          case Some(s) => s.fields.size
          case None => throw new SchemaException(s"Could not load Schema for second join relation")
        }
        val lList = if (lSize>1) (2 to lSize).toList else List.empty[Int]
        val rList = (1 to rSize).toList
        callST("join", Map("out"->out,"rel1"->rels.head,"key1"->keys.head,
          "rel2"->rels.tail.head,"key2"->keys.tail.head,"lsize"->lList,"rsize"->rList))  
      }
      case Union(out, rels) => callST("union", Map("out"->out,"in"->rels.head,"others"->rels.tail.mkString(","))) 
      case Sample(out, in, expr) => callST("sample", Map("out"->out,"in"->in.head,"expr"->expr)) //TODO
      case OrderBy(out, in, orderSpec) => callST("orderBy", Map("out"->out,"in"->in.head,"spec"->orderSpec)) // TODO
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

  def emitHeader(scriptName: String): String = callST("init_code") + callST("begin_query", Map("name" -> scriptName))

  def emitFooter: String = callST("end_query", Map("name" -> "Starting Flink Query"))

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

class FlinkCompile extends Compile {
  override val codeGen = new FlinkGenCode
}
