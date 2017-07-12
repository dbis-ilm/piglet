package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.expr._
import dbis.piglet.op._
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.schema.Schema
import dbis.piglet.tools.logging.PigletLogging

/**
  * Created by kai on 06.12.16.
  */
class ForeachEmitter extends CodeEmitter[Foreach] with PigletLogging {

  override def template: String =
    """val <out> = <in>.map{t =>
      |<if (profiling)>
      |if(scala.util.Random.nextInt(randFactor) == 0) {
      |  accum.incr("<lineage>", t.getNumBytes)
      |}
      |<endif>
      |<class>(<expr>)}""".stripMargin
  def templateNested: String =
    """val <out> = <in>.map{t =>
      |<if (profiling)>
      |if(scala.util.Random.nextInt(randFactor) == 0) {
      |  accum.incr("<lineage>", t.getNumBytes)
      |}
      |<endif>
      |<expr>}""".stripMargin

  def templateFlatMap: String =
    """val <out> = <in>.flatMap{t =>
      |<if (profiling)>
      |if(scala.util.Random.nextInt(randFactor) == 0) {
      |  accum.incr("<lineage>", t.getNumBytes)
      |}
      |<endif>
      |<expr>}""".stripMargin

  override def code(ctx: CodeGenContext, op: Foreach): String = {
    if (op.schema.isEmpty)
      throw CodeGenException("FOREACH requires a schema definition")
    
    val className: String = ScalaEmitter.schemaClassName(op.schema.get.className)

    val expr = emitForeachExpr(ctx, op)
    // in case of a nested FOREACH the tuples are creates as part of the GENERATE clause
    // -> no need to give the schema class
    if (op.generator.isInstanceOf[GeneratorPlan]) {
      CodeEmitter.render(templateNested, Map("out" -> op.outPipeName, "in" -> op.inPipeName, "expr" -> expr, "lineage"->op.lineageSignature))
    }
    else {
      // we need to know if the generator contains flatten on tuples or on bags (which require flatMap)
      val requiresFlatMap = op.containsFlatten(onBag = true)
      if (requiresFlatMap)
        CodeEmitter.render(templateFlatMap, Map("out" -> op.outPipeName, "in" -> op.inPipeName, "expr" -> expr, "lineage"->op.lineageSignature))
      else
        render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "class" -> className, "expr" -> expr, "lineage"->op.lineageSignature))
    }
  }

  def emitForeachExpr(ctx: CodeGenContext, op: Foreach): String = {
    // we need to know if the generator contains flatten on tuples or on bags (which require flatMap)
    // val requiresPlainFlatten =  op.asInstanceOf[Foreach].containsFlatten(onBag = false)
    val requiresFlatMap = op.containsFlatten(onBag = true)
    op.generator match {
      case GeneratorList(expr) =>
        if (requiresFlatMap)
          emitBagFlattenGenerator(CodeGenContext(ctx, Map("schema" -> op.inputSchema)), op, expr)
        else
          emitGenerator(CodeGenContext(ctx, Map("schema" -> op.inputSchema)), expr)
      case GeneratorPlan(plan) =>
        val subPlan = op.subPlan.get
        emitNestedPlan(ctx, parent = op, plan = subPlan)
    }
  }

  /**
    * Constructs the GENERATE expression list in FOREACH.
    *
    * @param ctx an object representing context information for code generation
    * @param genExprs the list of expressions in the GENERATE clause
    * @return a string representation of the Scala code
    */
  def emitGenerator(ctx: CodeGenContext, genExprs: List[GeneratorExpr]): String = {
    val ctx2 = CodeGenContext(ctx, Map("aggregate" -> false, "namedRef" -> true))
    s"${genExprs.map(e =>
      ScalaEmitter.emitExpr(ctx2, e.expr)).mkString(", ")}"
  }

  /**
    * Creates the Scala code needed for a flatten expression where the argument is a bag.
    * It requires a flatMap transformation.
    *
    * @param op the FOREACH operator containing the flatten in the GENERATE clause
    * @param genExprs the list of generator expressions
    * @return a string representation of the Scala code
    */
  def emitBagFlattenGenerator(ctx: CodeGenContext, op: Foreach, genExprs: List[GeneratorExpr]): String = {
    require(op.schema.isDefined)
    val className = ScalaEmitter.schemaClassName(op.schema.get.className)
    // extract the flatten expression from the generator list
    val flattenExprs = genExprs.filter(e => e.expr.traverseOr(op.inputSchema.orNull, Expr.containsFlattenOnBag))
    // determine the remaining expressions
    val otherExprs = genExprs.diff(flattenExprs)
    if (flattenExprs.size == 1) {
      // there is only a single flatten expression
      val ex: FlattenExpr = flattenExprs.head.expr.asInstanceOf[FlattenExpr]
      val ctx2 = CodeGenContext(ctx, Map("schema" -> op.inputSchema))
      if (otherExprs.nonEmpty) {
        // we have to cross join the flatten expression with the others:
        // t._1.map(s => <class>(<expr))
        val exs = otherExprs.map(e => ScalaEmitter.emitExpr(ctx2, e.expr)).mkString(",")
        s"${ScalaEmitter.emitExpr(ctx2, ex)}.map(s => $className($exs, s))"
      }
      else {
        // there is no other expression: we just construct an expression for flatMap:
        // (<expr>).map(t => <class>(t))
        s"${ScalaEmitter.emitExpr(ctx, ex.a)}.map(t => $className(t._0))"
      }
    }
    else
      s"" // i.flatMap(t => t(1).asInstanceOf[Seq[Any]].map(s => List(t(0),s)))
  }

  /**
    * Generates Scala code for a nested plan, i.e. statements within nested FOREACH.
    *
    * @param parent the parent FOREACH statement
    * @param plan the dataflow plan representing the nested statements
    * @return the generated code
    */
  def emitNestedPlan(ctx: CodeGenContext, parent: Foreach, plan: DataflowPlan): String = {
    val schema = parent.inputSchema
    require(parent.schema.isDefined)
    val className = ScalaEmitter.schemaClassName(parent.schema.get.className)

    "{\n" + plan.operators.map {
      case Generate(expr) => s"""$className(${emitGenerator(CodeGenContext(ctx, Map("schema" -> schema, "namedRef" -> true)), expr)})"""
      case n@ConstructBag(out, ref) => ref match {
        case DerefTuple(r1, r2) =>
          // there are two options of ConstructBag
          // 1. r1 refers to the input pipe of the outer operator (for automatically
          //    inserted ConstructBag operators)
          if (r1.toString == parent.inPipeName) {
            val pos = findFieldPosition(schema, r2)
            // println("pos = " + pos)
            s"""val ${n.outPipeName} = t._$pos.toList"""
          }
          else {
            // 2. r1 refers to a field in the schema
            val p1 = findFieldPosition(schema, r1)
            val p2 = findFieldPosition(ScalaEmitter.tupleSchema(schema, r1), r2)
            // println("pos2 = " + p1 + ", " + p2)
            s"""val ${n.outPipeName} = t._$p1.map(l => l._$p2).toList"""
          }
        case _ => "" // should not happen
      }
      case n@Distinct(out, in, _) => s"""val ${n.outPipeName} = ${n.inPipeName}.distinct"""
      case n@Filter(out, in, pred, _) =>
        val e = new FilterEmitter
        e.code(ctx, n)
        //        callST("filter", Map("out" -> n.outPipeName, "in" -> n.inPipeName,
        //          "pred" -> ScalaEmitter.emitPredicate(CodeGenContext(ctx, Map("schema" -> n.schema)), pred)))
      case n@Limit(out, in, num) =>
        val e = new LimitEmitter
        e.code(ctx, n) // ("limit", Map("out" -> n.outPipeName, "in" -> n.inPipeName, "num" -> num))
      case OrderBy(out, in, orderSpec, _) => throw CodeGenException("nested ORDER BY not implemented")
      case _ => ""
    }.mkString("\n") + "}"
  }

  /**
    * Find the index of the field represented by the reference in the given schema.
    * The reference could be a named field or a positional field. If not found -1 is returned.
    *
    * @param schema the schema containing the field
    * @param field the field denoted by a Ref object
    * @return the index of the field
    */
  def findFieldPosition(schema: Option[Schema], field: Ref): Int = field match {
    case nf @ NamedField(f, _) => schema match {
      case Some(s) => if (f == s.element.name) 0 else s.indexOfField(nf)
      case None => -1
    }
    case PositionalField(p) => p
    case _ => -1
  }

}

object ForeachEmitter {
  lazy val instance = new ForeachEmitter
}
