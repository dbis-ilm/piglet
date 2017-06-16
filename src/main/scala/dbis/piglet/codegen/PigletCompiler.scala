package dbis.piglet.codegen

import dbis.piglet.backends.BackendManager
import dbis.piglet.plan.DataflowPlan
import java.nio.file.Path

import dbis.piglet.tools.logging.PigletLogging
import dbis.piglet.parser.PigParser
import dbis.piglet.plan.rewriting.Rewriter._
import dbis.piglet.parser.PigParser
import dbis.piglet.op.PigOperator
import dbis.piglet.tools.FileTools
import dbis.piglet.tools.ScalaCompiler
import dbis.piglet.tools.JarBuilder
import dbis.piglet.tools.CppCompiler
import dbis.piglet.tools.CppCompilerConf
import dbis.piglet.tools.Conf
import dbis.setm.SETM.timing
import java.nio.file.Paths
import java.nio.file.Files
import java.io.FileWriter

import scala.io.Source

import scalax.file.{Path => xPath}
import dbis.piglet.schema.Schema
import java.net.URI

import dbis.piglet.tools.CliParams
import dbis.piglet.expr.Expr
import dbis.piglet.mm.DataflowProfiler


object PigletCompiler extends PigletLogging {

  /**
   * Helper method to parse the given file into a dataflow plan
   *
   * @param inputFile The file to parse
   * @param params Key value pairs to replace placeholders in the script
   * @param backend The name of the backend
   */
  def createDataflowPlan(inputFile: Path, params: Map[String,String], backend: String): Option[DataflowPlan] = timing("create DFP") {
    // 1. we read the Pig file
    val source = Source.fromFile(inputFile.toFile)

    logger.debug( s"""loaded pig script from "$inputFile" """)

    // 2. then we parse it and construct a dataflow plan
    val plan = new DataflowPlan(parseScriptFromSource(source, params))

    if (!plan.checkConnectivity) {
      logger.error(s"dataflow plan not connected for $inputFile")
      None
    }
    else {
      logger.debug(s"successfully created dataflow plan for $inputFile")
      Some(plan)
    }
  }

  /**
   * Replace placeholders in the script with values provided by the given map
   *
   * @param line The line to process
   * @param params The map of placeholder key and the value to use as replacement
   */
  def replaceParameters(line: String, params: Map[String,String]): String = {
    var s = line
    params.foreach{case (p, v) => s = s.replaceAll("\\$" + p, v)}
    s
  }

  /**
   * Handle IMPORT statements by simply replacing the line containing IMPORT with the content
   * of the imported file.
   *
   * @param lines the original script
   * @return the script where IMPORTs are replaced
   */
  def resolveImports(lines: Iterator[String]): (Iterator[String], Map[String,String]) = {
    var params = Map[String,String]()
    val buf = collection.mutable.ListBuffer.empty[String]
    for (l <- lines) {
      if (l.matches("""[ \t]*[iI][mM][pP][oO][rR][tT][ \t]*'([^'\p{Cntrl}\\]|\\[\\"bfnrt]|\\u[a-fA-F0-9]{4})*'[ \t\n]*;""")) {
        val s = l.split(" ")(1)
        val name = s.substring(1, s.length - 2)
        val path = Paths.get(name)
        val (resolvedLine, newParams) = resolveImports(loadScript(path))
        buf ++= resolvedLine
        params ++= newParams
      }
      else if (l.trim.toLowerCase.startsWith("%declare")) {
        val res = l.split(" ")
        if (res.length >= 2)
          params += (res(1) -> res(2).stripSuffix(";"))
      }
      else
          buf += l
    }
    (buf.toIterator, params)
  }


  /**
   * Compile the given plan into a executable program.
   *
   * @param plan The plan to compile
   * @param scriptName The name of the script (used as program and file name)
   * @param c Paramters
   */
  def compilePlan(plan: DataflowPlan, scriptName: String, c: CliParams): Option[Path] = timing("compile plan") {

    // compile it into Scala code for Spark
    val generatorClass = Conf.backendGenerator(c.backend)
    logger.debug(s"using generator class: $generatorClass")
    val extension = Conf.backendExtension(c.backend)
    logger.debug(s"file extension for generated code: $extension")
//    val args = Array(templateFile).asInstanceOf[Array[AnyRef]]
//    logger.debug(s"""arguments to generator class: "${args.mkString(",")}" """)

    val codeGenStrategy = Class.forName(generatorClass).getConstructors()(0).newInstance().asInstanceOf[CodeGenStrategy]
    val codeGenerator = CodeGenerator(codeGenStrategy)

    logger.debug(s"successfully created code generator class ${codeGenerator.getClass.getName}")

    // generate the Scala code
    val code = codeGenerator.generate(scriptName, plan, if(c.profiling.isDefined) Some(DataflowProfiler.url) else None)

    logger.debug("successfully generated program source code")

    // write it to a file

    val outputDir = c.outDir.resolve(scriptName) //new File(s"$outDir${File.separator}${scriptName}")

    logger.debug(s"outputDir: $outputDir")

    if (!Files.exists(outputDir)) {
      Files.createDirectories(outputDir)
    }


    val outputDirectory = outputDir.resolve("out") //s"${outputDir.getCanonicalPath}${File.separator}out"
    logger.debug(s"outputDirectory: $outputDirectory")

    // check whether output directory exists
    if (!Files.exists(outputDirectory)) {
      Files.createDirectory(outputDirectory)
    }

    val outputFile = outputDirectory.resolve(s"$scriptName.$extension")
    logger.debug(s"outputFile: $outputFile")
    
    val writer = new FileWriter(outputFile.toFile)
    writer.append(code)
    writer.close()
    
    if (extension.equalsIgnoreCase("scala")) {
      
      val libraryJars = collection.mutable.ArrayBuffer(
          c.backendPath.resolve(Conf.backendJar(c.backend)).toString, // the backend library (sparklib, flinklib, etc)
          c.backendPath.resolve(Conf.commonJar).toString) // common lib
      
      // if the plan contains geometry expressions, we need to add the spatial library           
      if(plan.checkExpressions(Expr.containsGeometryType))
        libraryJars += c.backendPath.resolve(Conf.spatialJar).toString
          
      // extract all additional jar files to output
      (libraryJars ++= plan.additionalJars).foreach{jarFile => 
        FileTools.extractJarToDir(jarFile, outputDirectory)
      }

      val sources = List(outputFile)

      // compile the scala code
      if (!ScalaCompiler.compile(outputDirectory, sources))
        return None

      // build a jar file
      logger.debug(s"creating job's jar file ... ")
    
      val jarFile = outputDir.resolve(s"$scriptName.jar")
      if (JarBuilder(outputDirectory, jarFile, verbose = false)) {
        logger.debug(s"created job's jar file at $jarFile")

        if(!c.keepFiles) {
          // remove directory $outputDirectory
          FileTools.recursiveDelete(outputDirectory)
        }
        Some(jarFile)
      } else
        None
    }
    else if (CppCompiler.compile(outputDirectory.toString, outputFile.toString, CppCompilerConf.cppConf(c.backend))) {
      logger.info(s"created job's file at $outputFile")
      Some(outputFile)
    }
    else
      None
  }

  /**
   * Load a Piglet script from the given file and return it as a Iterator on line strings.
   *
   * @param inputFile the path to the input file
   * @return the text lines
   */
  private def loadScript(inputFile: Path): Iterator[String] = Source.fromFile(inputFile.toFile).getLines()

   /**
    * Invokes the PigParser to process the given source. In addition, parameters specified by the --param flag
    * are resolved.
    *
    * @param source the source referring to the Piglet script
    * @param params a map of parameters
    * @return a list of PigOperators constructed from parsing the script
    */
  private def parseScriptFromSource(source: Source, params: Map[String,String]): List[PigOperator] = timing("parse script from source") {
    // Handle IMPORT and %DECLARE statements.
	  val (sourceLines, declareParams) = resolveImports(source.getLines())
	  if(declareParams.nonEmpty)
      logger.info("declared parameters: " + declareParams.mkString(", "))

      
    val allParams = params ++ declareParams
    
	  if (allParams.nonEmpty) {
	    // Replace placeholders by parameters.
		  PigParser.parseScript(sourceLines.map(line => replaceParameters(line, allParams)).mkString("\n"))
	  }
	  else {
		  PigParser.parseScript(sourceLines.mkString("\n"))
	  }
  }

  

}
