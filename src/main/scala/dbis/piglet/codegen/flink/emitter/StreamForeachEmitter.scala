package dbis.piglet.codegen.flink.emitter

import dbis.piglet.codegen.{ CodeEmitter, CodeGenContext, CodeGenException }
import dbis.piglet.expr._
import dbis.piglet.op._
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.schema.Schema
import dbis.piglet.codegen.scala_lang.ScalaEmitter
import dbis.piglet.udf.UDFTable
import dbis.piglet.codegen.scala_lang.ForeachEmitter

class StreamForeachEmitter extends ForeachEmitter {
  override def template: String = """    val <out> = <in>.map(t => <class>(<expr>))""".stripMargin
  override def templateNested: String = """    val <out> = <in>.map(t => <expr>)""".stripMargin
  override def templateFlatMap: String = """    val <out> = <in>.flatMap(t => <expr>)""".stripMargin
  def templateHelper: String = """    .foreach { t => out.collect(<class>(<expr>)) }""".stripMargin

  override def helper(ctx: CodeGenContext, op: Foreach): String = {
    if (!op.windowMode && containsAggregates(op.generator)) {
      val genL: GeneratorList = op.generator match {
        case gl @ GeneratorList(expr) => gl
        case GeneratorPlan(plan) => GeneratorList(plan.last.asInstanceOf[Generate].exprs)
      }
      val e = new StreamAccumulateEmitter
      val acc = new Accumulate(op.outputs.head, op.inputs.head, genL)
      acc.constructSchema
      e.helper(ctx, acc)
    } else ""
  }

  def windowApply(ctx: CodeGenContext, op: Foreach): String = {
    var params = Map[String, Any]()
    params += "expr" -> emitForeachExpr(ctx, op, false)
    if (!op.generator.isNested) params += "class" -> ScalaEmitter.schemaClassName(op.schema.get.className)
    CodeEmitter.render(templateHelper, params)
  }

  override def code(ctx: CodeGenContext, op: Foreach): String = {
    if (op.windowMode) return ""
    if (!op.schema.isDefined)
      throw CodeGenException("FOREACH requires a schema definition")

    val className = ScalaEmitter.schemaClassName(op.schema.get.className)
    val aggr = !op.windowMode && containsAggregates(op.generator)
    val expr = emitForeachExpr(ctx, op, aggr)

    val requiresFlatMap = op.asInstanceOf[Foreach].containsFlatten(onBag = true)
    if (aggr) expr
    else if (requiresFlatMap)
      CodeEmitter.render(templateFlatMap, Map("out" -> op.outPipeName, "in" -> op.inPipeName, "expr" -> expr, "class" -> className))
    else
      render(Map("out" -> op.outPipeName, "in" -> op.inPipeName, "expr" -> expr, "class" -> className))
  }

  def emitForeachExpr(ctx: CodeGenContext, op: Foreach, aggr: Boolean): String = {
    // we need to know if the generator contains flatten on tuples or on bags (which require flatMap)
    val requiresPlainFlatten = op.asInstanceOf[Foreach].containsFlatten(onBag = false)
    val requiresFlatMap = op.containsFlatten(onBag = true)
    op.generator match {
      case gl @ GeneratorList(expr) => {
        if (requiresFlatMap)
          emitBagFlattenGenerator(CodeGenContext(ctx, Map("schema" -> op.inputSchema)), op, expr)
        else {
          if (aggr) {
            val e = new StreamAccumulateEmitter
            val acc = new Accumulate(op.outputs.head, op.inputs.head, gl)
            acc.constructSchema
            e.code(ctx, acc)
          } else
            emitGenerator(CodeGenContext(ctx, Map("schema" -> op.inputSchema)), expr)
        }
      }
      case GeneratorPlan(plan) => {
        val subPlan = op.subPlan.get
        emitNestedPlan(ctx, parent = op, plan = subPlan, aggr)
      }
    }
  }

  /**
   * Generates Scala code for a nested plan, i.e. statements within nested FOREACH.
   *
   * @param parent the parent FOREACH statement
   * @param plan the dataflow plan representing the nested statements
   * @param aggr generate clause contains aggregates
   * @return the generated code
   */
  def emitNestedPlan(ctx: CodeGenContext, parent: PigOperator, plan: DataflowPlan, aggr: Boolean): String = {
    val schema = parent.inputSchema
    require(parent.schema.isDefined)
    val className = ScalaEmitter.schemaClassName(parent.schema.get.className)

    "{\n" + plan.operators.map {
      case n @ Generate(expr) =>
        if (aggr) {
          val e = new StreamAccumulateEmitter
          val acc = new Accumulate(n.outputs.head, n.inputs.head, GeneratorList(expr))
          acc.constructSchema
          e.code(ctx, acc)
        } else s"""${className}(${emitGenerator(CodeGenContext(ctx, Map("schema" -> schema, "namedRef" -> true)), expr)})"""
      case n @ ConstructBag(out, ref) => ref match {
        case DerefTuple(r1, r2) => {
          // there are two options of ConstructBag
          // 1. r1 refers to the input pipe of the outer operator (for automatically
          //    inserted ConstructBag operators)
          if (r1.toString == parent.inPipeName) {
            val pos = findFieldPosition(schema, r2)
            // println("pos = " + pos)
            s"""val ${n.outPipeName} = t._$pos.toList"""
          } else {
            // 2. r1 refers to a field in the schema
            val p1 = findFieldPosition(schema, r1)
            val p2 = findFieldPosition(ScalaEmitter.tupleSchema(schema, r1), r2)
            // println("pos2 = " + p1 + ", " + p2)
            s"""val ${n.outPipeName} = t._$p1.map(l => l._$p2).toList"""
          }
        }
        case _ => "" // should not happen
      }
      case n @ Distinct(out, in, windowMode) => s"""val ${n.outPipeName} = ${n.inPipeName}.distinct"""
      case n @ Filter(out, in, pred, windowMode) => {
        val e = new StreamFilterEmitter
        e.code(ctx, n)
      }
      case OrderBy(out, in, orderSpec, windowMode) => throw CodeGenException("nested ORDER BY not implemented")
      case _ => ""
    }.mkString("\n") + "}"
  }

  def containsAggregates(gen: ForeachGenerator): Boolean = {
    var exprs = gen match {
      case GeneratorList(expr) => expr
      case GeneratorPlan(plan) => plan.last.asInstanceOf[Generate].exprs
    }
    exprs.foreach { e =>
      e.expr match {
        case Func(f, _) => UDFTable.findFirstUDF(f) match {
          case Some(udf) if udf.isAggregate => return true
          case _ =>
        }
        case _ =>
      }
    }
    return false
  }
}

object StreamForeachEmitter {
	lazy val instance = new StreamForeachEmitter
}