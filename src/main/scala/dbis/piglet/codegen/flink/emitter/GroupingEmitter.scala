package dbis.piglet.codegen.flink.emitter

import dbis.piglet.op.Grouping
import dbis.piglet.codegen.scala_lang.ScalaEmitter
import dbis.piglet.codegen.CodeGenException
import dbis.piglet.schema.TupleType
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.codegen.CodeEmitter

class GroupingEmitter extends CodeEmitter[Grouping] {
  override def template: String = """<if (expr)>
                                    |    val <out> = <in>.groupBy(t => <expr>).reduceGroup{ (in, out: Collector[<class>]) => val itr = in.toIterable; out.collect(<class>(<keyExtr>, itr)) }
                                    |<else>
                                    |    val <out> = <in>.setParallelism(1).mapPartition( (in, out: Collector[<class>]) =>  out.collect(<class>("all", in.toIterable)))
                                    |<endif>""".stripMargin

  override def code(ctx: CodeGenContext, op: Grouping): String = {
    if (!op.schema.isDefined)
      throw CodeGenException("schema required in GROUPING")
    val className = ScalaEmitter.schemaClassName(op.schema.get.className)

    // GROUP ALL: no need to generate a key
    if (op.groupExpr.keyList.isEmpty)
      render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "class" -> className))
    else {
      val keyExtr = if (op.groupExpr.keyList.size > 1) {
        // the grouping key consists of multiple fields, i.e. we have
        // to construct a tuple where the type is the TupleType of the group field
        val field = op.schema.get.field("group")
        val className = field.fType match {
          case TupleType(f, c) => ScalaEmitter.schemaClassName(c)
          case _ => throw CodeGenException("unknown type for GROUPING key")
        }
        s"${className}(" + op.groupExpr.keyList.map(e => ScalaEmitter.emitRef(CodeGenContext(ctx, Map("schema" -> op.inputSchema, "tuplePrefix" -> "itr.head")), e)).mkString(",") + ")"
      } else op.groupExpr.keyList.map(e => ScalaEmitter.emitRef(CodeGenContext(ctx, Map("schema" -> op.inputSchema, "tuplePrefix" -> "itr.head")), e)).mkString // the simple case: the key is a single field

      render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "class" -> className,
        "expr" -> ScalaEmitter.emitGroupExpr(CodeGenContext(ctx, Map("schema" -> op.inputSchema)), op.groupExpr),
        "keyExtr" -> keyExtr))
    }
  }
}

object GroupingEmitter {
	lazy val instance = new GroupingEmitter
}