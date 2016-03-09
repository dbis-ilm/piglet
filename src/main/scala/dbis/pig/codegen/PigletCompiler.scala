package dbis.pig.codegen

import dbis.pig.backends.BackendManager
import dbis.pig.plan.DataflowPlan
import dbis.pig.parser.LanguageFeature._
import java.nio.file.Path
import dbis.pig.tools.logging.PigletLogging
import dbis.pig.parser.PigParser
import dbis.pig.plan.rewriting.Rewriter._
import scala.collection.mutable
import scala.io.Source
import scala.collection.mutable.ListBuffer
import dbis.pig.parser.PigParser
import dbis.pig.op.PigOperator
import java.nio.file.Paths
import java.nio.file.Files
import java.io.FileWriter
import dbis.pig.tools.FileTools
import dbis.pig.tools.ScalaCompiler
import dbis.pig.tools.JarBuilder
import dbis.pig.tools.CppCompiler
import dbis.pig.tools.CppCompilerConf
import dbis.pig.tools.Conf
import de.tuilmenau.setm.SETM.timing
import scalax.file.{Path => xPath}


object PigletCompiler extends PigletLogging {
  
  /**
   * Helper method to parse the given file into a dataflow plan
   * 
   * @param inputFile The file to parse
   * @param params Key value pairs to replace placeholders in the script
   * @param backend The name of the backend
   * @param langFeatures the Pig dialects used for parsing
   */
  def createDataflowPlan(inputFile: Path, params: Map[String,String], backend: String,
                         langFeatures: List[LanguageFeature]): Option[DataflowPlan] = timing("create DFP") {
    // 1. we read the Pig file
    val source = Source.fromFile(inputFile.toFile)

    logger.debug( s"""loaded pig script from "$inputFile" """)

    // 2. then we parse it and construct a dataflow plan
    val plan = new DataflowPlan(parseScriptFromSource(source, params, langFeatures))

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
    * Helper method to parse the given Piglet script from a string into a dataflow plan
    *
    * @param input The file to parse
    * @param params Key value pairs to replace placeholders in the script
    * @param backend The name of the backend
    * @param langFeatures the Pig dialects used for parsing
    */
  def createDataflowPlan(input: String, params: Map[String,String], backend: String,
                         langFeatures: List[LanguageFeature]): Option[DataflowPlan] = {
    // 1. we prepare a source from the string
    val source = Source.fromString(input.stripMargin)
    // 2. then we parse it and construct a dataflow plan
    val plan = new DataflowPlan(parseScriptFromSource(source, params, langFeatures))

    if (!plan.checkConnectivity) {
      logger.error("dataflow plan not connected")
      None
    }
    else {
      logger.debug("successfully created dataflow plan from input")
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
    val buf = ListBuffer.empty[String]
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
 * Create Scala code for the given backend from the source string.
 * This method is provided mainly for Zeppelin.
 *
 * @param source the Piglet script
 * @param backend the backend used to compile and execute
 * @return the generated Scala code
 */
def createCodeFromInput(source: String, backend: String): String = {
  var plan = new DataflowPlan(PigParser.parseScript(source))

  if (!plan.checkConnectivity) {
    logger.error(s"dataflow plan not connected")
    return ""
  }

  logger.debug(s"successfully created dataflow plan")
  // plan = processPlan(plan)

  // compile it into Scala code for Spark
    val generatorClass = Conf.backendGenerator(backend)
  val extension = Conf.backendExtension(backend)
  val backendConf = BackendManager.backend(backend)
    BackendManager.backend = backendConf
  val templateFile = backendConf.templateFile
  val args = Array(templateFile).asInstanceOf[Array[AnyRef]]
  val compiler = Class.forName(generatorClass).getConstructors()(0).newInstance(args: _*).asInstanceOf[CodeGenerator]

  // 5. generate the Scala code
  val code = compiler.compile("blubs", plan, profiling = false, forREPL = true)
  logger.debug("successfully generated scala program")
  code
}


  /**
   * Compile the given plan into a executable program.
   * 
   * @param plan The plan to compile
   * @param scriptName The name of the script (used as program and file name)
   * @param outDir The directory to write generated files to
   * @param backendJar Path to the backend jar file
   * @param templateFile The template file to use for code generation
   * @param backend The name of the backend
   * @param profiling Flag indicating whether profiling code should be inserted
   * @param keepFiles Flag indicating whether generated source files should be deleted or kept
   */
  def compilePlan(plan: DataflowPlan, scriptName: String, outDir: Path, backendJar: Path, 
      templateFile: String, backend: String, profiling: Boolean, keepFiles: Boolean): Option[Path] = timing("compile plan") {
    
    // compile it into Scala code for Spark
    val generatorClass = Conf.backendGenerator(backend)
    logger.debug(s"using generator class: $generatorClass")
    val extension = Conf.backendExtension(backend)
    logger.debug(s"file extension for generated code: $extension")
    val args = Array(templateFile).asInstanceOf[Array[AnyRef]]
    logger.debug(s"""arguments to generator class: "${args.mkString(",")}" """)
    
    val codeGenerator = Class.forName(generatorClass).getConstructors()(0).newInstance(args: _*).asInstanceOf[CodeGenerator]
    logger.debug(s"successfully created code generator class $codeGenerator")

    // generate the Scala code
    val code = codeGenerator.compile(scriptName, plan, profiling)

    logger.debug("successfully generated scala program")

    // write it to a file

    val outputDir = outDir.resolve(scriptName) //new File(s"$outDir${File.separator}${scriptName}")

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
      // extract all additional jar files to output
      plan.additionalJars.foreach(jarFile => FileTools.extractJarToDir(jarFile, outputDirectory))

      // copy the backend-specific library to output
      val jobJar = backendJar.toAbsolutePath.toString
      logger.info(s"add backend jar '${jobJar}' to job's jar file ...")
      FileTools.extractJarToDir(jobJar, outputDirectory)

      // copy the common library to output
      val commonJars = xPath("common/target/scala-2.11/",'/')  ** "*.jar"
      commonJars.foreach { jar => 
        logger.info(s"add common jar '${jar.path}' to job's jar file ...")
        FileTools.extractJarToDir(jar.path, outputDirectory)
      }

      val sources = ListBuffer(outputFile)
      
      // compile the scala code
      if (!ScalaCompiler.compile(outputDirectory, sources))
        return None

      // build a jar file
      logger.info(s"creating job's jar file ...")
      val jarFile = Paths.get(outDir.toAbsolutePath.toString, scriptName, s"$scriptName.jar")
      if (JarBuilder(outputDirectory, jarFile, verbose = false)) {
        logger.info(s"created job's jar file at $jarFile")

        if(!keepFiles) {
          // remove directory $outputDirectory
          val p = Paths.get(outDir.toAbsolutePath().toString(), scriptName, "out")
          cleanupResult(p)
        }
        Some(jarFile)
      } else
        None
    }
    else if (CppCompiler.compile(outputDirectory.toString, outputFile.toString, CppCompilerConf.cppConf(backend))) {
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
    * @param langFeatures the language dialects used to parse the script
    * @return a list of PigOperators constructed from parsing the script
    */
  private def parseScriptFromSource(source: Source, params: Map[String,String],
                  langFeatures: List[LanguageFeature]): List[PigOperator] = timing("parse script from source") {
    // Handle IMPORT and %DECLARE statements.
	  val (sourceLines, declareParams) = resolveImports(source.getLines())
    logger.info("declared parameters: " + declareParams.mkString(", "))
    val allParams = params ++ declareParams

	  if (allParams.nonEmpty) {
	    // Replace placeholders by parameters.
		  PigParser.parseScript(sourceLines.map(line => replaceParameters(line, allParams)).mkString("\n"), langFeatures)
	  }
	  else {
		  PigParser.parseScript(sourceLines.mkString("\n"), langFeatures)
	  }
  }

  private def cleanupResult(dir: Path): Unit = {
    val path = scalax.file.Path.fromString(dir.toString())
    try {
      path.deleteRecursively(continueOnFailure = false)
      logger.debug(s"removed output directory at $dir")
    }
    catch {
      case e: java.io.IOException => logger.debug(s"Could not remove result directory at ${path}: ${e.getMessage}")
    }
  }

}
