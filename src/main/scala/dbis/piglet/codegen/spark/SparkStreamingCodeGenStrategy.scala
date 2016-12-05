package dbis.piglet.codegen.spark

import java.net.URI

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.codegen.scala_lang.ScalaCodeGenStrategy
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.tools.Conf

/**
  * Created by kai on 05.12.16.
  */
class SparkStreamingCodeGenStrategy extends ScalaCodeGenStrategy {
  /**
    * Generate code needed for importing required Scala packages.
    *
    * @return a string representing the import code
    */
  override def emitImport(ctx: CodeGenContext, additionalImports: Seq[String] = Seq.empty): String =
    CodeEmitter.render("""import org.apache.spark._
                         |import org.apache.spark.streaming._
                         |import dbis.piglet.backends.{SchemaClass, Record}
                         |import dbis.piglet.tools._
                         |import dbis.piglet.backends.spark._
                         |<additional_imports>
                         |
                         |object SECONDS {
                         |  def apply(p: Long) = Seconds(p)
                         |}
                         |object MINUTES {
                         |  def apply(p: Long) = Minutes(p)
                         |}
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
        |   SparkStream.setAppName("<name>_App")
        |    val ssc = SparkStream.ssc
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
    var map = Map("name" -> scriptName)

    profiling.map { u => u.resolve(Conf.EXECTIMES_FRAGMENT).toString() }
      .foreach { s => map += ("profiling" -> s) }


    CodeEmitter.render("""    def main(args: Array[String]) {
                         |
                         |        <if (profiling)>
                         |        val perfMon = new PerfMonitor("<name>_App")
                         |        ssc.sparkContext.addSparkListener(perfMon)
                         |        <endif>
                         |""".stripMargin, map)
  }

  override def emitFooter(ctx: CodeGenContext, plan: DataflowPlan): String = {
    /*
     * We want to force the termination with the help of a timeout,
     * if all source nodes are Load operators as text files are not continuous.
     */
    var forceTermin = if(plan.operators.isEmpty) false else true
    plan.sourceNodes.foreach(op => forceTermin &= op.isInstanceOf[Load])
    var params = Map("name" -> "Starting Query")
    if (forceTermin) params += ("forceTermin" -> forceTermin.toString())
    CodeEmitter.render("""      ssc.start()
                         |	    ssc.awaitTermination<if (forceTermin)>OrTimeout(5000)<else>()<endif>
                         |    }
                         |}""".stripMargin, params)

  }
}
