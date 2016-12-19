package dbis.piglet.codegen.flink

import java.net.URI

import dbis.piglet.codegen.{ CodeEmitter, CodeGenContext }
import dbis.piglet.codegen.scala_lang.ScalaCodeGenStrategy
import dbis.piglet.op.Load
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.tools.Conf
import dbis.piglet.codegen.CodeGenTarget
import dbis.piglet.codegen.scala_lang.DumpEmitter
import dbis.piglet.codegen.scala_lang.LoadEmitter
import dbis.piglet.codegen.scala_lang.StoreEmitter
import dbis.piglet.op.PigOperator
import dbis.piglet.op.Dump
import dbis.piglet.op.Store

class FlinkCodeGenStrategy extends ScalaCodeGenStrategy {
  override val target = CodeGenTarget.FlinkStreaming
//  override val emitters = super.emitters + (
//    s"$pkg.Load" -> new FlinkLoadEmitter,
//    s"$pkg.Dump" -> new FlinkDumpEmitter,
//    s"$pkg.Store" -> new FlinkStoreEmitter
//  )
  
  
  override def emitterForNode[O <: PigOperator](op: O): CodeEmitter[O] = {
    
    val emitter = op match {
      case _: Load => new FlinkLoadEmitter
      case _: Dump => new FlinkDumpEmitter
      case _: Store => new FlinkStoreEmitter
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

  override def emitFooter(ctx: CodeGenContext, plan: DataflowPlan): String = {
    var params = Map("name" -> "Starting Query")
    CodeEmitter.render("""<if (hook)>
	                       |    shutdownHook()
                         |<endif>
                         |  }
                         |}""".stripMargin, params)

  }
}

/*------------------------------------------------------------------------------------------------- */
/*                                FlinkStreaming-specific emitters                                  */
/*------------------------------------------------------------------------------------------------- */

class FlinkLoadEmitter extends LoadEmitter {
  override def template: String = """    val <out> = <func>[<class>]().load(env, "<file>", <extractor><if (params)>, <params><endif>)""".stripMargin
}

class FlinkStoreEmitter extends StoreEmitter {
  override def template: String = """    <func>[<class>]().write("<file>", <in><if (params)>, <params><endif>)
                                    |    env.execute("Starting Query")""".stripMargin
}

class FlinkDumpEmitter extends DumpEmitter {
  override def template: String = """    <in>.map(_.mkString()).print""".stripMargin
}