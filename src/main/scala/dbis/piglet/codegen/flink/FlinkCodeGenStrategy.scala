package dbis.piglet.codegen.flink

import java.net.URI

import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.codegen.CodeGenTarget
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.op._
import dbis.piglet.codegen.flink.emitter._
import dbis.piglet.codegen.scala_lang.ScalaCodeGenStrategy
import dbis.piglet.codegen.CodeGenException
import dbis.piglet.expr.NamedField
import dbis.piglet.expr.PositionalField
import dbis.piglet.schema.Schema
import dbis.piglet.expr.Ref

class FlinkCodeGenStrategy extends ScalaCodeGenStrategy {
  override val target = CodeGenTarget.FlinkStreaming
  //  override val emitters = super.emitters + (
  //    s"$pkg.Load" -> FlinkLoadEmitter.instance,
  //    s"$pkg.Dump" -> FlinkDumpEmitter.instance,
  //    s"$pkg.Store" -> FlinkStoreEmitter.instance
  //  )

  override def emitterForNode[O <: PigOperator](op: O): CodeEmitter[O] = {

    val emitter = op match {
      case _: Load => LoadEmitter.instance
      case _: Dump => DumpEmitter.instance
      case _: Store => StoreEmitter.instance
      case _: Grouping => GroupingEmitter.instance
      case _: OrderBy => OrderByEmitter.instance
      case _: Join => JoinEmitter.instance
      case _: Limit => LimitEmitter.instance
      case _: StreamOp => StreamOpEmitter.instance
      case _: Accumulate => AccumulateEmitter.instance
      case _ => super.emitterForNode(op)
    }

    emitter.asInstanceOf[CodeEmitter[O]]
  }

  /**
   * Generate code needed for importing required Scala packages.
   *
   * @return a string representing the import code
   */
  override def emitImport(ctx: CodeGenContext, additionalImports: Seq[String] = Seq.empty): String =
    CodeEmitter.render("""import org.apache.flink.api.scala._
                         |import dbis.piglet.backends.flink._
                         |import dbis.piglet.backends.{SchemaClass, Record}
                         |import org.apache.flink.util.Collector
                         |import org.apache.flink.api.common.operators.Order
                         |import dbis.piglet.backends.flink.Sampler._
                         |<if (additional_imports)>
                         |<additional_imports>
                         |<endif>
                         |
                         |<\n>
                         |""".stripMargin,
      Map("additional_imports" -> additionalImports.mkString("\n")))

  /**
   * Generate code for the header of the script outside the main class/object,
   * i.e. defining the main object.
   *
   * @param scriptName the name of the script (e.g. used for the object)
   * @return a string representing the header code
   */
  override def emitHeader1(ctx: CodeGenContext, scriptName: String): String =
    CodeEmitter.render(
      """object <name> {
        |  val env = ExecutionEnvironment.getExecutionEnvironment<\n>
        |""".stripMargin, Map("name" -> scriptName))

  /**
   *
   * Generate code for the header of the script which should be defined inside
   * the main class/object.
   *
   * @param scriptName the name of the script (e.g. used for the object)
   * @param profiling add profiling code to the generated code
   * @return a string representing the header code
   */
  override def emitHeader2(ctx: CodeGenContext, scriptName: String, profiling: Option[URI] = None): String = {
    CodeEmitter.render("""  def main(args: Array[String]) {<\n>""", Map.empty)
  }

  override def emitFooter(ctx: CodeGenContext, plan: DataflowPlan, profiling: Option[URI] = None): String = {
    var params = Map("name" -> "Starting Query")
    CodeEmitter.render("""<if (hook)>
	                       |    shutdownHook()
                         |<endif>
                         |  }
                         |}""".stripMargin, params)

  }
}