package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.expr.{NamedField, PositionalField, Ref}
import dbis.piglet.op.{OrderByDirection, OrderBySpec, PigOperator, Top}
import dbis.piglet.schema.{Schema, Types}

/**
  * Created by kai on 12.12.16.
  */
class TopEmitter extends CodeEmitter {
  override def template: String = """val <out> = sc.parallelize(<in>.top(<num>)(custKey_<out>_<in>))""".stripMargin

  def helperTemplate: String = """object <params.cname> extends Ordering[<params.schemaclass>] {
                                 |        def compare(first: <params.schemaclass>, second: <params.schemaclass>): Int = <params.cmpExpr>
                                 |    }""".stripMargin

  /**
    * Returns true if the sort order of the OrderBySpec is ascending
    *
    * @param spec the OrderBySpec value
    * @return true if sorting in ascending order
    */
  def ascendingSortOrder(spec: OrderBySpec): String = if (spec.dir == OrderByDirection.AscendingOrder) "true" else "false"

  /**
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
      s"custKey_${out}_${in}(${orderSpec.map(r => ScalaEmitter.emitRef(ctx, r.field)).mkString(",")})"
  }

   override def helper(ctx: CodeGenContext, node: PigOperator): String = node match {
    case Top(_, _, orderSpec, _) => {
      /**
        * Bytearray fields need special handling: they are mapped to Any which is not comparable.
        * Thus we add ".toString" in this case.
        *
        * @param col the column used in comparison
        * @return ".toString" or ""
        */
      def genImplicitCast(col: Int): String = node.schema match {
        case Some(s) => if (s.field(col).fType == Types.ByteArrayType) ".toString" else ""
        case None => ".toString"
      }

      val size = orderSpec.size
      var params = Map[String, Any]()
      val hasSchema = node.inputSchema.isDefined

      val schemaClass = if (!hasSchema) {
        "Record"
      } else {
        ScalaEmitter.schemaClassName(node.schema.get.className)
      }

      params += "schemaclass" -> schemaClass

      def emitRefAccessor(schema: Option[Schema], ref: Ref) = ref match {
        case NamedField(f, _) if schema.isEmpty =>
          throw new CodeGenException(s"invalid field name $f")
        case nf@NamedField(f, _) if schema.get.indexOfField(nf) == -1 =>
          throw new CodeGenException(s"invalid field name $f")
        case nf: NamedField => schema.get.indexOfField(nf)
        case p@PositionalField(pos) => pos
        case _ => throw new CodeGenException(s"invalid ordering field $ref")
      }

      def genCmpExpr(col: Int): String = {
        var firstGetter = "first."
        var secondGetter = "second."
        if (ascendingSortOrder(orderSpec(col)) == "true") {
          // If we're not sorting ascending, reverse the getters so the ordering gets reversed
          firstGetter = "second."
          secondGetter = "first."
        }

        if (!hasSchema) {
          firstGetter += "get"
          secondGetter += "get"
        }

        val colname = emitRefAccessor(node.inputSchema, orderSpec(col).field)

        val cast = genImplicitCast(col)
        if (hasSchema) {
          if (col == (size - 1))
            s"{ ${firstGetter}_${colname}$cast compare ${secondGetter}_${colname}$cast }"
          else
            s"{ if (${firstGetter}_${colname} == ${secondGetter}_${colname}) ${genCmpExpr(col + 1)} else ${firstGetter}_${colname}$cast compare " +
              s"${secondGetter}_${colname}$cast }"
        } else {
          if ( {
            colname
          } == (size - 1))
            s"{ $firstGetter(${colname})$cast compare $secondGetter(${colname})$cast }"
          else
            s"{ if ($firstGetter(${colname}) == $secondGetter(${colname})) ${genCmpExpr(col + 1)} else $firstGetter(${colname})$cast compare " +
              s"$secondGetter(${colname})$cast }"
        }
      }

      //Spark
      params += "cname" -> s"custKey_${node.outPipeName}_${node.inPipeName}"
      var col = 0
      params += "fields" -> orderSpec.map(o => {
        col += 1;
        s"c$col: ${ScalaEmitter.scalaTypeOfField(o.field, node.schema)}"
      }).mkString(", ")
      params += "cmpExpr" -> genCmpExpr(0)

      //Flink
      params += "out" -> node.outPipeName
      params += "key" -> orderSpec.map(r => ScalaEmitter.emitRef(CodeGenContext(ctx, Map("schema" -> node.schema, "tuplePrefix" -> "t")), r.field)).mkString(",")
      if (ascendingSortOrder(orderSpec.head) == "false") params += "reverse" -> true
      CodeEmitter.render(helperTemplate, Map("params" -> params))
    }
  }

    override def code(ctx: CodeGenContext, node: PigOperator): String = {
    node match {
      case Top(_, _, orderSpec, num) => {
        val key = emitSortKey(CodeGenContext(ctx, Map("schema" -> node.schema)), orderSpec, node.outPipeName, node.inPipeName)
        val asc = ascendingSortOrder(orderSpec.head)
        render(Map("out" -> node.outPipeName, "in" -> node.inPipeName, "num" -> num, "key" -> key))
      }
      case _ => throw CodeGenException(s"unexpected operator: $node")
    }
  }

}
