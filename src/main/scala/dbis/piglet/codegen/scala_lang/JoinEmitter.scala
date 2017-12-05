package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.expr.Ref
import dbis.piglet.op.Join

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by kai on 12.12.16.
  */
class JoinEmitter extends CodeEmitter[Join] {
//
  override def template: String =
    """val <out>_hp = new org.apache.spark.HashPartitioner(32)
      |val <out> = <rel1>_kv.partitionBy(<out>_hp).join(<rel2>_kv.partitionBy(<out>_hp)).map{
      |  case (k,(v,w)) =>
      |    val res = <class>(<fields>)
      |    <if (profiling)>
      |      PerfMonitor.sampleSize(res,"<lineage>", accum, randFactor)
      |    <endif>
      |    res
      |}""".stripMargin


  def mwayJoinTemplate: String =
    """val <out> = <rel1>_kv<rel2: {rel | .join(<rel>_kv)}>.map{
      |  case (k,<pairs>) =>
      |    val res = <class>(<fields>)
      |    <if (profiling)>
      |      PerfMonitor.sampleSize(res,"<lineage>", accum, randFactor)
      |    <endif>
      |    res
      |}""".stripMargin


  def keymapTemplate: String =
    """<rels,keys:{ rel,key |val <rel>_kv = <rel>.map(t => (<key>,t))
      | }>""".stripMargin

  /**
    *
    * @param ctx an object representing context information for code generation
    * @param joinExpr The expression to compare elements with
    * @return Returns the string representing the join key field
    */
  def emitJoinKey(ctx: CodeGenContext, joinExpr: List[Ref]): String = {
    if (joinExpr.size == 1)
      ScalaEmitter.emitRef(ctx, joinExpr.head)
    else
      s"Array(${joinExpr.map(e => ScalaEmitter.emitRef(ctx, e)).mkString(",")}).mkString"
  }

  override def code(ctx: CodeGenContext, op: Join): String = {
    if (op.schema.isEmpty)
      throw CodeGenException("schema required in JOIN")

    val rels = op.inputs
    val res = rels.zip(op.fieldExprs)
    val keys = res.map { case (i, k) => emitJoinKey(CodeGenContext(ctx, Map("schema" -> i.producer.schema)), k) }

    /*
     * We don't generate key-value RDDs which we have already created and registered in joinKeyVars.
     * Thus, we build a list of 1 and 0's where 1 stands for a relation name for which we have already
     * created a _kv variable.
     */
    val duplicates = rels.map(r => if (JoinEmitter.joinKeyVars.contains(r.name)) 1 else 0)

    /*
     * Now we build lists for rels and keys by removing the elements corresponding to 1's in the duplicate
     * list.
     */
    val drels = rels.zipWithIndex.filter { r => duplicates(r._2) == 0 }.map(_._1)
    val dkeys = keys.zipWithIndex.filter { k => duplicates(k._2) == 0 }.map(_._1)

    /*
     * And finally, create the join kv vars for them...
     */
    var str = CodeEmitter.render(keymapTemplate, Map("rels" -> drels.map(_.name), "keys" -> dkeys)) + "\n"

    /*
     * We construct a string v._0, v._1 ... w._0, w._1 ...
     * The numbers of v's and w's are determined by the size of the input schemas.
     */
    val className = op.schema match {
      case Some(s) => ScalaEmitter.schemaClassName(s.className)
      case None => ScalaEmitter.schemaClassName(op.outPipeName)
    }

    /*
      *  ...as well as the actual join.
      */
    if (rels.length == 2) {
      val vsize = rels.head.inputSchema.get.fields.length
      val fieldList = op.schema.get.fields.zipWithIndex
        .map { case (f, i) => if (i < vsize) s"v._$i" else s"w._${i - vsize}" }.mkString(", ")

      str += render(
        Map("out" -> op.outPipeName,
          "rel1" -> rels.head.name,
          "class" -> className,
          "rel2" -> rels.tail.map(_.name),
          "fields" -> fieldList,
          "lineage" -> op.lineageSignature)) + "\n"
    }
    else {
      var pairs = "(v1,v2)"
      for (i <- 3 to rels.length) {
        pairs = s"($pairs,v$i)"
      }
      val fieldList = ArrayBuffer[String]()
      for (i <- 1 to op.inputs.length) {
        op.inputs(i - 1).producer.schema match {
          case Some(s) => fieldList ++= s.fields.zipWithIndex.map { case (f, k) => s"v$i._$k" }
          case None => fieldList += s"v$i._0"
        }
      }

      str += CodeEmitter.render(mwayJoinTemplate,
        Map("out" -> op.outPipeName,
          "rel1" -> rels.head.name,
          "class" -> className,
          "rel2" -> rels.tail.map(_.name),
          "pairs" -> pairs,
          "fields" -> fieldList.mkString(", "),
          "lineage" -> op.lineageSignature)) + "\n"
    }
    JoinEmitter.joinKeyVars += rels.head.name
    JoinEmitter.joinKeyVars ++= rels.tail.map(_.name)

    str
  }

}


object JoinEmitter {
  val joinKeyVars = mutable.Set[String]()
  
  lazy val instance = new JoinEmitter
}