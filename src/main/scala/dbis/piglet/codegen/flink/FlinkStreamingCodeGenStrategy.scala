package dbis.piglet.codegen.flink

import java.net.URI

import dbis.piglet.Piglet.Lineage
import dbis.piglet.codegen.flink.emitter._
import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenTarget}
import dbis.piglet.mm.ProfilerSettings
import dbis.piglet.op._
import dbis.piglet.plan.DataflowPlan


class FlinkStreamingCodeGenStrategy extends FlinkCodeGenStrategy {
  override val target = CodeGenTarget.FlinkStreaming

  override def emitterForNode[O <: PigOperator](op: O): CodeEmitter[O] = {

    val emitter = op match {
      case _: Load => StreamLoadEmitter.instance
      case _: Store => StreamStoreEmitter.instance
      case _: SocketRead => SocketReadEmitter.instance
      case _: SocketWrite => SocketWriteEmitter.instance
      case _: Filter => StreamFilterEmitter.instance
      case _: Foreach => StreamForeachEmitter.instance
      case _: Grouping => StreamGroupingEmitter.instance
      case _: OrderBy => StreamOrderByEmitter.instance
      case _: Accumulate => StreamAccumulateEmitter.instance
      case _: Join => StreamJoinEmitter.instance
      case _: Cross => StreamCrossEmitter.instance
      case _: Window => StreamWindowEmitter.instance
      case _: WindowApply => StreamWindowApplyEmitter.instance
      case _: Distinct => StreamDistinctEmitter.instance
      case _: Sample => StreamSampleEmitter.instance
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
  override def emitHeader2(ctx: CodeGenContext, scriptName: String, profiling: Option[ProfilerSettings] = None, operators: Seq[Lineage] = Seq.empty): String = {
    CodeEmitter.render("""  def main(args: Array[String]) {<\n>""", Map.empty)
  }

  override def emitFooter(ctx: CodeGenContext, plan: DataflowPlan, profiling: Option[URI] = None, operators:Seq[Lineage]=Seq.empty): String = {
    val params = Map("name" -> "Starting Query")
    CodeEmitter.render("""    env.execute("<name>")
                         |<if (hook)>
	                       |    shutdownHook()
                         |<endif>
                         |  }
                         |}""".stripMargin, params)

  }
}