package dbis.pig

/**
 * Created by kai on 10.04.15.
 */

class SparkGenCode extends GenCodeBase {
  def emitRef(schema: Option[Schema], ref: Ref): String = ref match {
    case NamedField(f) => {
      if (schema.isEmpty) throw new SchemaException(s"unknown schema for field $f")
      s"t._${schema.get.indexOfField(f)}"
    } // TODO: should be position of field
    case PositionalField(pos) => { s"t($pos)" }
    case Value(v) => { v.toString } // TODO: could be also a predicate!
    case _ => { "" }
  }

  def emitPredicate(schema: Option[Schema], predicate: Predicate): String = predicate match {
    case Eq(left, right) => { s"${emitRef(schema, left)} == ${emitRef(schema, right)}"}
    case Neq(left, right) => { s"${emitRef(schema, left)} != ${emitRef(schema, right)}"}
    case Leq(left, right) => { s"${emitRef(schema, left)} <= ${emitRef(schema, right)}"}
    case Lt(left, right) => { s"${emitRef(schema, left)} < ${emitRef(schema, right)}"}
    case Geq(left, right) => { s"${emitRef(schema, left)} >= ${emitRef(schema, right)}"}
    case Gt(left, right) => { s"${emitRef(schema, left)} > ${emitRef(schema, right)}"}
    case _ => { "" }
  }

  def emitGrouping(schema: Option[Schema], groupingExpr: GroupingExpression): String = {
    groupingExpr.keyList.map(e => emitRef(schema, e)).mkString(",")
  }

  def emitJoinKey(schema: Option[Schema], joinExpr: List[Ref]): String = {
    if (joinExpr.size == 1)
      emitRef(schema, joinExpr(0))
    else
      s"Array(${joinExpr.map(e => emitRef(schema, e)).mkString(",")}).mkString"
  }

  def quote(s: String): String = s""""$s""""

  def emitLoader(file: String, loaderFunc: String, loaderParams: List[String]): String = {
    if (loaderFunc == "")
      s"""sc.textFile("$file")"""
    else {
      var params = if (loaderParams != null && loaderParams.nonEmpty) ", " + loaderParams.map(quote(_)).mkString(",") else ""
      s"""${loaderFunc}().load(sc, "${file}"${params})"""
    }
  }

  def emitNode(node: PigOperator): String = node match {
    case Load(out, file, func, params) => { s"""val $out = ${emitLoader(file, func, params)}""" }
    case Dump(in) => { s"""${node.inPipeNames(0)}.collect.map(t => println(t.mkString(",")))""" }
    case Store(in, file) => { s"""${node.inPipeNames(0)}.coalesce(1, true).saveAsTextFile("${file}")""" }
    case Describe(in) => { s"$in: { $node.schemaToString }" }
    case Filter(out, in, pred) => { s"val $out = ${node.inPipeNames(0)}.filter(t => {${emitPredicate(node.schema, pred)}})" }
    case Grouping(out, in, groupExpr) => {
      if (groupExpr.keyList.isEmpty) s"val $out = ${node.inPipeNames(0)}.glom"
      else s"val $out = ${node.inPipeNames(0)}.groupBy(t => {${emitGrouping(node.schema, groupExpr)}})" }
    case Distinct(out, in) => { s"val $out = ${node.inPipeNames(0)}.distinct" }
    case Limit(out, in, num) => { s"val $out = sc.parallelize(${node.inPipeNames(0)}.take($num))" }
    case Join(out, rels, exprs) => {
      val res = rels.zip(exprs)
      val s1 = res.map{case (rel, expr) => s"val ${rel}_kv = ${rel}.keyBy(t => {${emitJoinKey(node.schema, expr)}})\n"}.mkString
      s1 + s"val $out = ${rels.head}_kv" + rels.tail.map{other => s".join(${other}_kv)"}.mkString
    }
    case _ => { "" }
  }

  def emitHeader(scriptName: String): String = {
    s"""
       |import org.apache.spark.SparkContext
       |import org.apache.spark.SparkContext._
       |import org.apache.spark.SparkConf
       |import org.apache.spark.rdd._
       |import dbis.spark._
       |
       |object $scriptName {
       |    def main(args: Array[String]) {
       |      val conf = new SparkConf().setAppName("${scriptName}_App")
       |      conf.setMaster("local[4]")
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