/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dbis.piglet.codegen.spark

import java.net.URI

import dbis.piglet.Piglet.Lineage
import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.codegen.scala_lang.ScalaCodeGenStrategy
import dbis.piglet.op.Load
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.tools.Conf
import dbis.piglet.codegen.CodeGenTarget
import dbis.piglet.op.PigOperator
import dbis.piglet.op.Distinct
import dbis.piglet.op.Window
import dbis.piglet.op.SocketRead
import dbis.piglet.op.OrderBy
import dbis.piglet.op.Grouping
import dbis.piglet.op.Store
import dbis.piglet.op.Dump

class SparkStreamingCodeGenStrategy extends ScalaCodeGenStrategy {
  override val target = CodeGenTarget.SparkStreaming

//  override val emitters = super.emitters + (
//    s"$pkg.Load" -> new StreamLoadEmitter,
//    s"$pkg.Dump" -> new StreamDumpEmitter,
//    s"$pkg.Store" -> new StreamStoreEmitter,
//    s"$pkg.Grouping" -> new StreamGroupingEmitter,
//    s"$pkg.OrderBy" -> new StreamOrderByEmitter,
//    s"$pkg.Distinct" -> new StreamDistinctEmitter,
//    s"$pkg.Window" -> new StreamWindowEmitter,
//    s"$pkg.SocketRead" -> new StreamSocketReadEmitter
//    )
  
  override def emitterForNode[O <: PigOperator](op: O): CodeEmitter[O] = {
    
    val emitter = op match {
      case _: Load => StreamLoadEmitter.instance
      case _: Dump => StreamDumpEmitter.instance
      case _: Store => StreamStoreEmitter.instance
      case _: Grouping => StreamGroupingEmitter.instance
      case _: OrderBy => StreamOrderByEmitter.instance
      case _: Distinct => StreamDistinctEmitter.instance
      case _: Window => StreamWindowEmitter.instance
      case _: SocketRead => StreamSocketReadEmitter.instance
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
        |  SparkStream.setAppName("<name>_App")
        |  val ssc = SparkStream.ssc
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
    var map = Map("name" -> scriptName)

    profiling.map { u => u.resolve(Conf.EXECTIMES_FRAGMENT).toString }
      .foreach { s => map += ("profiling" -> s) }


    CodeEmitter.render("""<\n>
                         |  def main(args: Array[String]) {
                         |<if (profiling)>
                         |    val perfMon = new PerfMonitor("<name>_App")
                         |    ssc.sparkContext.addSparkListener(perfMon)
                         |<endif>
                         |""".stripMargin, map)
  }

  override def emitFooter(ctx: CodeGenContext, plan: DataflowPlan, profiling: Option[URI] = None, operators:Seq[Lineage]=Seq.empty): String = {
    /*
     * We want to force the termination with the help of a timeout,
     * if all source nodes are Load operators as text files are not continuous.
     */
    var forceTermin = if(plan.operators.isEmpty) false else true
    plan.sourceNodes.foreach(op => forceTermin &= op.isInstanceOf[Load])
    var params = Map("name" -> "Starting Query")
    if (forceTermin) params += ("forceTermin" -> forceTermin.toString)
    CodeEmitter.render("""    ssc.start()
                         |	  ssc.awaitTermination<if (forceTermin)>OrTimeout(10000)<else>()<endif>
                         |  }
                         |}""".stripMargin, params)

  }
}
