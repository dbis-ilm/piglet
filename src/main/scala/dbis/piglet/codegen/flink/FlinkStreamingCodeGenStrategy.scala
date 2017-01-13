package dbis.piglet.codegen.flink

import java.net.URI

import dbis.piglet.op._
import dbis.piglet.codegen.CodeEmitter
import dbis.piglet.codegen.CodeGenContext
import dbis.piglet.codegen.CodeGenTarget
import dbis.piglet.codegen.flink.emitter._
import dbis.piglet.codegen.scala_lang.ScalaCodeGenStrategy
import dbis.piglet.plan.DataflowPlan


class FlinkStreamingCodeGenStrategy extends FlinkCodeGenStrategy {
  override val target = CodeGenTarget.FlinkStreaming
  //  override val emitters = super.emitters + (
  //    s"$pkg.Load" -> new FlinkStreamingLoadEmitter,
  //    s"$pkg.Dump" -> new FlinkStreamingDumpEmitter,
  //    s"$pkg.Store" -> new FlinkStreamingStoreEmitter
  //  )

  override def emitterForNode[O <: PigOperator](op: O): CodeEmitter[O] = {

    val emitter = op match {
      case _: Load => new StreamLoadEmitter
      case _: Store => new StreamStoreEmitter
      case _: SocketRead => new SocketReadEmitter
      case _: SocketWrite => new SocketWriteEmitter
      case _: Filter => new StreamFilterEmitter
      case _: Foreach => new StreamForeachEmitter
      case _: Grouping => new StreamGroupingEmitter
      case _: OrderBy => new StreamOrderByEmitter
      case _: Accumulate => new StreamAccumulateEmitter
      case _: Join => new StreamJoinEmitter
      case _: Cross => new StreamCrossEmitter
      case _: Window => new StreamWindowEmitter
      case _: WindowApply => new StreamWindowApplyEmitter
      case _: Distinct => new StreamDistinctEmitter
      case _: Sample => new StreamSampleEmitter
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
    CodeEmitter.render("""import org.apache.flink.streaming.api.scala._
                         |import dbis.piglet.backends.flink._
                         |import dbis.piglet.backends.flink.streaming._
                         |import java.util.concurrent.TimeUnit
                         |import org.apache.flink.util.Collector
                         |import org.apache.flink.streaming.api.windowing.assigners._
                         |import org.apache.flink.streaming.api.windowing.evictors._
                         |import org.apache.flink.streaming.api.windowing.time._
                         |import org.apache.flink.streaming.api.windowing.triggers._
                         |import org.apache.flink.streaming.api.windowing.windows._
                         |import org.apache.flink.streaming.api.TimeCharacteristic
                         |import dbis.piglet.backends.{SchemaClass, Record}
                         |<if (additional_imports)>
                         |<additional_imports>
                         |<endif>
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
        |  val env = StreamExecutionEnvironment.getExecutionEnvironment
        |  env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
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

  override def emitFooter(ctx: CodeGenContext, plan: DataflowPlan): String = {
    var params = Map("name" -> "Starting Query")
    CodeEmitter.render("""    env.execute("<name>")
                         |<if (hook)>
	                       |    shutdownHook()
                         |<endif>
                         |  }
                         |}""".stripMargin, params)

  }
}