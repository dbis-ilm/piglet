package dbis.piglet.codegen.scala_lang

import java.net.URI

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenStrategy, CodeGenTarget}
import dbis.piglet.expr.Expr
import dbis.piglet.op.PigOperator
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.schema._
import dbis.piglet.tools.Conf

import scala.collection.mutable.ListBuffer

/**
  * Created by kai on 02.12.16.
  */
abstract class ScalaCodeGenStrategy extends CodeGenStrategy {
  // initialize target and emitters
  val target = CodeGenTarget.Unknown
  val pkg = "dbis.piglet.op"

  def emitters: Map[String, CodeEmitter] = Map[String, CodeEmitter](
    s"$pkg.Load" -> new LoadEmitter,
    s"$pkg.Filter" -> new FilterEmitter,
    s"$pkg.Limit" -> new LimitEmitter,
    s"$pkg.Foreach" -> new ForeachEmitter,
    s"$pkg.Distinct" -> new DistinctEmitter,
    s"$pkg.Sample" -> new SampleEmitter,
    s"$pkg.Union" -> new UnionEmitter,
    s"$pkg.Grouping" -> new GroupingEmitter,
    s"$pkg.OrderBy" -> new OrderByEmitter,
    s"$pkg.Dump" -> new DumpEmitter,
    s"$pkg.Empty" -> new EmptyEmitter,
    s"$pkg.Store" -> new StoreEmitter
  )

  override def collectAdditionalImports(plan: DataflowPlan) = {
    val additionalImports = ListBuffer.empty[String]
    if (plan.checkExpressions(Expr.containsMatrixType)) {
      additionalImports += "import breeze.linalg._"
    }

    if (plan.checkExpressions(Expr.containsGeometryType)) {
      additionalImports ++= Seq(
        "import com.vividsolutions.jts.io.WKTReader",
        "import dbis.stark.{STObject, Instant, Interval}",
        "import dbis.stark.STObject._",
        "import dbis.stark.spatial.SpatialRDD._")
    }
    additionalImports
  }


  /**
    * Generate code for embedded code: usually this code is just copied
    * to the generated file.
    *
    * @param additionalCode the code to be embedded
    * @return a string representing the code
    */
  def emitEmbeddedCode(ctx: CodeGenContext, additionalCode: String) = additionalCode

  def emitNode(ctx: CodeGenContext, node: PigOperator): String = {
    val emitter: CodeEmitter = emitterForNode(node.getClass.getName)

    var code = emitter.beforeCode(ctx, node)
    if (code.length > 0) code += "\n"

    code += emitter.code(ctx, node)

    val afterCode = emitter.afterCode(ctx, node)
    if (afterCode.length > 0)
      code += "\n" + afterCode

    code
  }


  /**
    * Generate code for classes representing schema types.
    *
    * @param schemas the list of schemas for which we generate classes
    * @return a string representing the code
    */
  override def emitSchemaHelpers(ctx: CodeGenContext, schemas: List[Schema]): String = {
    var converterCode = ""

    val classes = ListBuffer.empty[(String, String)]

    for (schema <- schemas) {
      val values = ScalaEmitter.createSchemaInfo(schema)

      classes += ScalaEmitter.emitSchemaClass(values)
      converterCode += ScalaEmitter.emitSchemaConverters(values)
    }

    val p = "_t([0-9]+)_Tuple".r

    val sortedClasses = classes.sortWith { case (left, right) =>
      val leftNum = left._1 match {
        case p(group) => group.toInt
        case _ => throw new IllegalArgumentException(s"unexpected class name: $left")
      }

      val rightNum = right._1 match {
        case p(group) => group.toInt
        case _ => throw new IllegalArgumentException(s"unexpected class name: $left")
      }

      leftNum < rightNum
    }

    val classCode = sortedClasses.map(_._2).mkString("\n")

    classCode + "\n" + converterCode
  }

  /**
    * Generate code for any helper class/function if needed by the given operator.
    *
    * @param node the Pig operator requiring helper code
    * @return a string representing the helper code
    */
  override def emitHelperClass(ctx: CodeGenContext, node: PigOperator): String = {
    val emitter: CodeEmitter = emitterForNode(node.getClass.getName)

    emitter.helper(ctx, node)
  }
}
