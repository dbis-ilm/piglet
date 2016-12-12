package dbis.piglet.codegen.spark

import java.net.URI

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.codegen.scala_lang.ScalaCodeGenStrategy
import dbis.piglet.op.Load
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.tools.Conf
import dbis.piglet.codegen.CodeGenTarget
import dbis.piglet.codegen.scala_lang.LoadEmitter
import dbis.piglet.codegen.scala_lang.StoreEmitter
import dbis.piglet.codegen.scala_lang.DumpEmitter

class SparkCodeGenStrategy extends ScalaCodeGenStrategy {
  override val target = CodeGenTarget.Spark

  /**
    * Generate code needed for importing required Scala packages.
    *
    * @return a string representing the import code
    */
  override def emitImport(ctx: CodeGenContext, additionalImports: Seq[String] = Seq.empty): String =
    CodeEmitter.render("""import org.apache.spark.SparkContext
                         |import org.apache.spark.SparkContext._
                         |import org.apache.spark.SparkConf
                         |import org.apache.spark.rdd._
                         |import dbis.piglet.backends.{SchemaClass, Record}
                         |import dbis.piglet.tools._
                         |import dbis.piglet.backends.spark._
                         |<additional_imports>
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
        |val conf = new SparkConf().setAppName("<name>_App")
        |val sc = new SparkContext(conf)
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


    CodeEmitter.render("""  def main(args: Array[String]) {
                         |
                         |<if (profiling)>
                         |    val perfMon = new PerfMonitor("<name>_App","<profiling>")
                         |    sc.addSparkListener(perfMon)
                         |<endif>
                         |""".stripMargin, map)
  }

  override def emitFooter(ctx: CodeGenContext, plan: DataflowPlan): String = {
      CodeEmitter.render("""    sc.stop()
                         |  }
                         |}""".stripMargin, Map("name" -> "Starting Query"))

  }
}


/*------------------------------------------------------------------------------------------------- */
/*                                SparkStreaming-specific emitters                                  */
/*------------------------------------------------------------------------------------------------- */

class StreamLoadEmitter extends LoadEmitter {
  override def template: String = """    val <out> = <func>[<class>]().loadStream(ssc, "<file>", <extractor><if (params)>, <params><endif>)""".stripMargin
}

class StreamStoreEmitter extends StoreEmitter {
  override def template: String = """    <func>[<class>]().writeStream("<file>", <in><if (params)>, <params><endif>)""".stripMargin
}

class StreamDumpEmitter extends DumpEmitter {
  override def template: String = """    <in>.foreachRDD(rdd => rdd.foreach(elem => println(elem.mkString())))""".stripMargin
}