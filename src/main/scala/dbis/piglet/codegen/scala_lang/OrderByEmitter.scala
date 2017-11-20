package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.{OrderBy, OrderByDirection, OrderBySpec}
import dbis.piglet.schema.Types

/**
  * Created by kai on 09.12.16.
  */
class OrderByEmitter extends CodeEmitter[OrderBy] {
  override def template: String =
    """val <out> = <in>.keyBy(t => <key>).sortByKey(<asc>).map{case (_,v) =>
      |  <if (profiling)>
      |  if(scala.util.Random.nextInt(randFactor) == 0) {
      |    PerfMonitor.sampleSize(v,"<lineage>", accum)
      |  }
      |  <endif>
      |  v
      |}""".stripMargin

  def helperTemplate: String = """case class <params.cname>(<params.fields>) extends Ordered[<params.cname>] {
                                 |        def compare(that: <params.cname>) = <params.cmpExpr>
                                 |    }""".stripMargin
  /**
    * Returns true if the sort order of the OrderBySpec is ascending
    *
    * @param spec the OrderBySpec value
    * @return true if sorting in ascending order
    */
  def ascendingSortOrder(spec: OrderBySpec): String = if (spec.dir == OrderByDirection.AscendingOrder) "true" else "false"


  /**
    *
    * @param ctx an object representing context information for code generation
    * @param orderSpec
    * @param out
    * @param in
    * @return
    */
  def emitSortKey(ctx: CodeGenContext, orderSpec: List[OrderBySpec], out: String, in: String) : String = {
    if (orderSpec.size == 1)
      ScalaEmitter.emitRef(ctx, orderSpec.head.field)
    else
      s"custKey_${out}_$in(${orderSpec.map(r => ScalaEmitter.emitRef(ctx, r.field)).mkString(",")})"
  }

  override def helper(ctx: CodeGenContext, op: OrderBy): String = {
      /*
        * Bytearray fields need special handling: they are mapped to Any which is not comparable.
        * Thus we add ".toString" in this case.
        *
        * @param col the column used in comparison
        * @return ".toString" or ""
        */
      def genImplicitCast(col: Int): String = op.schema match {
        case Some(s) => if (s.field(col).fType == Types.ByteArrayType) ".toString" else ""
        case None => ".toString"
      }

      val num = op.orderSpec.length

      /*
        * Emit the comparison expression used in in the orderHelper class
        *
        * @param col the current position of the comparison field
        * @return the expression code
        */
      def genCmpExpr(col: Int): String = {
        val cast = genImplicitCast(col - 1)
        val cmpStr = if (op.orderSpec(col - 1).dir == OrderByDirection.AscendingOrder)
          s"this.c$col$cast compare that.c$col$cast"
        else s"that.c$col$cast compare this.c$col$cast"
        if (col == num) s"{ $cmpStr }"
        else s"{ if (this.c$col == that.c$col) ${genCmpExpr(col + 1)} else $cmpStr }"
      }

      var params = Map[String, Any]()
      //Spark
      params += "cname" -> s"custKey_${op.outPipeName}_${op.inPipeName}"
      var col = 0
      params += "fields" -> op.orderSpec.map(o => {
        col += 1
        s"c$col: ${ScalaEmitter.scalaTypeOfField(o.field, op.schema)}"
      }).mkString(", ")
      params += "cmpExpr" -> genCmpExpr(1)

      //Flink??
      params += "out" -> op.outPipeName
      params += "key" -> op.orderSpec.map(r => ScalaEmitter.emitRef(CodeGenContext(ctx, Map("schema" -> op.schema, "tuplePrefix" -> "t")), r.field)).mkString(",")
      if (ascendingSortOrder(op.orderSpec.head) == "false") params += "reverse" -> true

      CodeEmitter.render(helperTemplate, Map("params" -> params))

  }


  override def code(ctx: CodeGenContext, op: OrderBy): String = {
    val key = emitSortKey(CodeGenContext(ctx, Map("schema" -> op.schema)), op.orderSpec, op.outPipeName, op.inPipeName)
    val asc = ascendingSortOrder(op.orderSpec.head)
    render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "key" -> key, "asc" -> asc, "lineage" -> op.lineageSignature))
  }

}

/**
 * @author hg
 */
object OrderByEmitter {
  lazy val instance = new OrderByEmitter
}