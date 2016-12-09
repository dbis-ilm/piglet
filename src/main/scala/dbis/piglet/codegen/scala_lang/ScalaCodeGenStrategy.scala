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
class ScalaCodeGenStrategy extends CodeGenStrategy {
  // initialize target and emitters
  val target = CodeGenTarget.Spark
  val pkg = "dbis.piglet.op"

  val emitters: Map[String, CodeEmitter] = Map[String, CodeEmitter](
    s"$pkg.Load" -> new LoadEmitter,
    s"$pkg.Filter" -> new FilterEmitter,
    s"$pkg.Limit" -> new LimitEmitter,
    s"$pkg.Foreach" -> new ForeachEmitter,
    s"$pkg.Distinct" -> new DistinctEmitter,
    s"$pkg.Sample" -> new SampleEmitter,
    s"$pkg.Union" -> new UnionEmitter,
    s"$pkg.Grouping" -> new GroupingEmitter,
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
    * Generate code needed for importing required Scala packages.
    *
    * @return a string representing the import code
    */
  def emitImport(ctx: CodeGenContext, additionalImports: Seq[String] = Seq.empty): String =
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
  def emitHeader1(ctx: CodeGenContext, scriptName: String): String =
    CodeEmitter.render(
      """object <name> {
        |val conf = new SparkConf().setAppName("<name>_App")
        |val sc = new SparkContext(conf)
        |""".stripMargin, Map("name" -> scriptName))

  /**
    * Generate code for embedded code: usually this code is just copied
    * to the generated file.
    *
    * @param additionalCode the code to be embedded
    * @return a string representing the code
    */
  def emitEmbeddedCode(ctx: CodeGenContext, additionalCode: String) = additionalCode

  /**
    *
    * Generate code for the header of the script which should be defined inside
    * the main class/object.
    *
    * @param scriptName the name of the script (e.g. used for the object)
    * @param profiling add profiling code to the generated code
    * @return a string representing the header code
    */
  def emitHeader2(ctx: CodeGenContext, scriptName: String, profiling: Option[URI] = None): String = {
    var map = Map("name" -> scriptName)

    profiling.map { u => u.resolve(Conf.EXECTIMES_FRAGMENT).toString() }
      .foreach { s => map += ("profiling" -> s) }


    CodeEmitter.render(""" def main(args: Array[String]) {
                         |
                         |        <if (profiling)>
                         |    	val perfMon = new PerfMonitor("<name>_App","<profiling>")
                         |    	sc.addSparkListener(perfMon)
                         |    	<endif>
                         |""".stripMargin, map)
  }

  /**
    * Generate code needed for finishing the script and starting the execution.
    *
    * @param plan the dataflow plan for which we generate the code
    * @return a string representing the end of the code.
    */
  def emitFooter(ctx: CodeGenContext, plan: DataflowPlan): String =
    CodeEmitter.render("""sc.stop()
                         |    }
                         |}""".stripMargin, Map("name" -> "Starting Query"))

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
    * Generate code for a class representing a schema type.
    *
    * @param values
    * @return
    */
  private def emitSchemaClass(values: (String, String, String, String, String)): (String, String) = {
    val (name, fieldNames, fieldTypes, fieldStr, toStr) = values

    val code = CodeEmitter.render("""case class <name> (<fields>) extends java.io.Serializable with SchemaClass {
                                    |  override def mkString(_c: String = ",") = <string_rep>
                                    |}
                                    |""".stripMargin, Map("name" -> name,
      "fieldNames" -> fieldNames,
      "fieldTypes" -> fieldTypes,
      "fields"   -> fieldStr,
      "string_rep" -> toStr))

    (name, code)
  }

  private def emitSchemaConverters(values: (String, String, String, String, String)): String = {
    val (name, fieldNames, fieldTypes, _, _) = values

    CodeEmitter.render("""<if (fieldNames)>
                         |implicit def convert<name>(t: (<fieldTypes>)): <name> = <name>(<fieldNames>)
                         |<endif>""".stripMargin, Map("name" -> name,
      "fieldNames" -> fieldNames,
      "fieldTypes" -> fieldTypes
    ))
  }

  private def createSchemaInfo(schema: Schema) = {
    def typeName(f: PigType, n: String) = ScalaEmitter.scalaTypeMappingTable.get(f) match {
      case Some(n) => n
      case None => f match {
        // if we have a bag without a name then we assume that we have got
        // a case class with _<field_name>_Tuple
        case BagType(v) => s"Iterable[_${v.className}_Tuple]"
        case TupleType(f, c) => ScalaEmitter.schemaClassName(c)
        case MapType(v) => s"Map[String,${ScalaEmitter.scalaTypeMappingTable(v)}]"
        case MatrixType(v, rows, cols, rep) => s"DenseMatrix[${if (v.tc == TypeCode.IntType) "Int" else "Double"}]"
        case _ => f.descriptionString
      }
    }
    val fieldList = schema.fields.toList
    // build the list of field names (_0, ..., _n)
    val fieldNames = if (fieldList.size==1) "t" else fieldList.indices.map(t => "t._"+(t+1)).mkString(", ")
    val fieldTypes = fieldList.map(f => s"${typeName(f.fType, f.name)}").mkString(", ")
    val fields = fieldList.zipWithIndex.map{ case (f, i) =>
      (s"_$i", s"${typeName(f.fType, f.name)}")}
    val fieldStr = fields.map(t => t._1 + ": " + t._2).mkString(", ")

    // construct the mkString method
    //   we have to handle the different types here:
    //      TupleType -> ()
    //      BagType -> {}
    val toStr = fieldList.zipWithIndex.map{
      case (f, i) => f.fType match {
        case BagType(_) => s""""{" + _$i.mkString(",") + "}""""
        case MapType(v) => s""""[" + _$i.map{ case (k,v) => s"$$k#$$v" }.mkString(",") + "]""""
        case _ => s"_$i" + (if (f.fType.tc != TypeCode.CharArrayType && fields.length == 1) ".toString" else "")
      }
    }.mkString(" + _c + ")

    val name = ScalaEmitter.schemaClassName(schema.className)

    (name, fieldNames, fieldTypes, fieldStr, toStr)
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
      val values = createSchemaInfo(schema)

      classes += emitSchemaClass(values)
      converterCode += emitSchemaConverters(values)
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
