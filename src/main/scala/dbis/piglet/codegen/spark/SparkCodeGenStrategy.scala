package dbis.piglet.codegen.spark

import java.net.URI

import dbis.piglet.Piglet.Lineage
import dbis.piglet.codegen.scala_lang.ScalaCodeGenStrategy
import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenTarget}
import dbis.piglet.expr.Expr
import dbis.piglet.op._
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.tools.Conf

class SparkCodeGenStrategy extends ScalaCodeGenStrategy {
  override val target = CodeGenTarget.Spark


  override def collectAdditionalImports(plan: DataflowPlan) = {
    val additionalImports = super.collectAdditionalImports(plan)

    if (plan.checkExpressions(Expr.containsGeometryType)) {
      additionalImports ++= Seq(
        "import com.vividsolutions.jts.io.WKTReader",
        "import dbis.stark.{STObject, Instant, Interval}",
        "import dbis.stark.STObject._",
        "import dbis.stark.spatial._",
        "import dbis.stark.spatial.SpatialRDD._")
    }
    additionalImports
  }
  
  override def emitterForNode[O <: PigOperator](op: O): CodeEmitter[O] = {
    val emitter = op match {
      case _: SpatialFilter => SpatialFilterEmitter.instance
      case _: SpatialJoin => SpatialJoinEmitter.instance
      case _: IndexOp => SpatialIndexEmitter.instance
      case _: Partition => PartitionerEmitter.instance
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
  override def emitHeader2(ctx: CodeGenContext, scriptName: String, profiling: Option[URI] = None, operators: Seq[Lineage] = Seq.empty): String = {
    var map = Map[String,Any]("name" -> scriptName)

    profiling.foreach { p =>
      val t = p.resolve(Conf.EXECTIMES_FRAGMENT).toString
      val s = p.resolve(Conf.SIZES_FRAGMENT).toString

//      map += ("profiling" -> t)
      map += ("profilingTimes" -> t)
      map += ("profilingSizes" -> s)
      map += ("lineages" -> operators)
    }


    CodeEmitter.render(s"""  def main(args: Array[String]) {
                         |<if (profiling)>
                         |  val url = "<profilingTimes>"
                         |  val sizesUrl = "<profilingSizes>"
                         |  PerfMonitor.notify(url,"progstart",null,-1,System.currentTimeMillis)
                         |<endif>
                         |val conf = new SparkConf().setAppName("<name>_App")
                    		 |val sc = new SparkContext(conf)
                         |<if (profiling)>
                         |    <lineages:{ l | val accum_<l> = new dbis.piglet.backends.spark.SizeAccumulator()
                         |    sc.register(accum_<l>,"accum_<l>")
                         |     }>
                         |
                         |    PerfMonitor.notify(url,"start",null,-1,System.currentTimeMillis)
                         |<endif>
                         |""".stripMargin, map)
  }

  override def emitFooter(ctx: CodeGenContext, plan: DataflowPlan, profiling: Option[URI] = None, operators: Seq[Lineage] = Seq.empty): String = {

    val map = profiling.map { u => 
      val url = u.resolve(Conf.EXECTIMES_FRAGMENT).toString
      Map("profiling" -> url,"lineages"->operators)
    }.getOrElse(Map.empty[String,String])
      
      CodeEmitter.render(

        s"""
         |<if (profiling)>
         |  val m = scala.collection.mutable.Map.empty[String,Option[(Long,Long)]]
         |  <lineages:{ l | m("<l>") = accum_<l>.value
         |  }>
         |  PerfMonitor.sizes(sizesUrl,m)
         |<endif>
         |  sc.stop()
         |<if (profiling)>
         |    PerfMonitor.notify(url,"end",null,-1,System.currentTimeMillis)
         |<endif>
         |  }
         |}""".stripMargin, map)

  }
}
