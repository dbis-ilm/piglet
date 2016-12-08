package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.expr._
import dbis.piglet.op._
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.schema.Schema

/**
  * Created by kai on 06.12.16.
  */
class ForeachEmitter extends CodeEmitter {
  override def template: String = """val <out> = <in>.map(t => <class>(<expr>))""".stripMargin
  def templateNested: String = """val <out> = <in>.map(t => <expr>)""".stripMargin
  def templateFlatMap: String = """val <out> = <in>.flatMap(t => <expr>)""".stripMargin

  override def code(ctx: CodeGenContext, node: PigOperator): String = {
    node match {
      case Foreach(out, in, gen, _) => {
        if (!node.schema.isDefined)
          throw CodeGenException("FOREACH requires a schema definition")
        val className: String = ScalaEmitter.schemaClassName(node.schema.get.className)
        val expr = emitForeachExpr(ctx, node, gen)
        // in case of a nested FOREACH the tuples are creates as part of the GENERATE clause
        // -> no need to give the schema class
        if (gen.isInstanceOf[GeneratorPlan]) {
          CodeEmitter.render(templateNested, Map("out" -> node.outPipeName, "in" -> node.inPipeName, "expr" -> expr))
        }
        else {
          // we need to know if the generator contains flatten on tuples or on bags (which require flatMap)
          val requiresFlatMap = node.asInstanceOf[Foreach].containsFlatten(onBag = true)
          if (requiresFlatMap)
            CodeEmitter.render(templateFlatMap, Map("out" -> node.outPipeName, "in" -> node.inPipeName, "expr" -> expr))
          else
            render(Map("out" -> node.outPipeName, "in" -> node.inPipeName, "class" -> className, "expr" -> expr))
        }
      }
      case _ => throw CodeGenException(s"unexpected operator: $node")
    }
  }

  def emitForeachExpr(ctx: CodeGenContext, node: PigOperator, gen: ForeachGenerator): String = {
    // we need to know if the generator contains flatten on tuples or on bags (which require flatMap)
    // val requiresPlainFlatten =  node.asInstanceOf[Foreach].containsFlatten(onBag = false)
    val requiresFlatMap = node.asInstanceOf[Foreach].containsFlatten(onBag = true)
    gen match {
      case GeneratorList(expr) => {
        if (requiresFlatMap)
          emitBagFlattenGenerator(ctx, node, expr)
        else
          emitGenerator(CodeGenContext(ctx, Map("schema" -> node.inputSchema)), expr)
      }
      case GeneratorPlan(plan) => {
        val subPlan = node.asInstanceOf[Foreach].subPlan.get
        emitNestedPlan(ctx, parent = node, plan = subPlan)
      }
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
    * @param node the FOREACH operator containing the flatten in the GENERATE clause
    * @param genExprs the list of generator expressions
    * @return a string representation of the Scala code
    */
  def emitBagFlattenGenerator(ctx: CodeGenContext, node: PigOperator, genExprs: List[GeneratorExpr]): String = {
    require(node.schema.isDefined)
    val className = ScalaEmitter.schemaClassName(node.schema.get.className)
    // extract the flatten expression from the generator list
    val flattenExprs = genExprs.filter(e => e.expr.traverseOr(node.inputSchema.getOrElse(null), Expr.containsFlattenOnBag))
    // determine the remaining expressions
    val otherExprs = genExprs.diff(flattenExprs)
    if (flattenExprs.size == 1) {
      // there is only a single flatten expression
      val ex: FlattenExpr = flattenExprs.head.expr.asInstanceOf[FlattenExpr]
      val ctx2 = CodeGenContext(ctx, Map("schema" -> node.inputSchema))
      if (otherExprs.nonEmpty) {
        // we have to cross join the flatten expression with the others:
        // t._1.map(s => <class>(<expr))
        val exs = otherExprs.map(e => ScalaEmitter.emitExpr(ctx2, e.expr)).mkString(",")
        s"${ScalaEmitter.emitExpr(ctx2, ex)}.map(s => ${className}($exs, s))"
      }
      else {
        // there is no other expression: we just construct an expression for flatMap:
        // (<expr>).map(t => <class>(t))
        s"${ScalaEmitter.emitExpr(ctx, ex.a)}.map(t => ${className}(t._0))"
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
  def emitNestedPlan(ctx: CodeGenContext, parent: PigOperator, plan: DataflowPlan): String = {
    val schema = parent.inputSchema
    require(parent.schema.isDefined)
    val className = ScalaEmitter.schemaClassName(parent.schema.get.className)

    "{\n" + plan.operators.map {
      case Generate(expr) => s"""${className}(${emitGenerator(CodeGenContext(ctx, Map("schema" -> schema, "namedRef" -> true)), expr)})"""
      case n@ConstructBag(out, ref) => ref match {
        case DerefTuple(r1, r2) => {
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
        }
        case _ => "" // should not happen
      }
      case n@Distinct(out, in, _) => s"""val ${n.outPipeName} = ${n.inPipeName}.distinct"""
      case n@Filter(out, in, pred, _) => {
        val e = new FilterEmitter
        e.code(ctx, n)
//        callST("filter", Map("out" -> n.outPipeName, "in" -> n.inPipeName,
//          "pred" -> ScalaEmitter.emitPredicate(CodeGenContext(ctx, Map("schema" -> n.schema)), pred)))
      }
      case n@Limit(out, in, num) => {
        val e = new LimitEmitter
        e.code(ctx, n)
      } // ("limit", Map("out" -> n.outPipeName, "in" -> n.inPipeName, "num" -> num))
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
