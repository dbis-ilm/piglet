package dbis.pig

import java.io.{FileReader, FileWriter}

import org.apache.spark.deploy.SparkSubmit

import scala.io.Source
import scala.util.parsing.input.CharSequenceReader

/**
 * Created by kai on 31.03.15.
 */
object PigCompiler extends PigParser {
  def parseScript(s: CharSequence): List[PigOperator] = {
    parseScript(new CharSequenceReader(s))
  }

  def parseScript(input: CharSequenceReader): List[PigOperator] = {
    parsePhrase(input) match {
      case Success(t, _) => t
      case NoSuccess(msg, next) => throw new IllegalArgumentException(
        "Could not parse '" + input + "' near '" + next.pos.longString + ": " + msg)
    }
  }

  def parseScriptFromSource(source: Source): List[PigOperator] = {
    parseScript(source.getLines().mkString)
  }

  def parsePhrase(input: CharSequenceReader): ParseResult[List[PigOperator]] = {
    phrase(script)(input)
  }

  case class CompilerConfig(master: String = "local", input: String = "", compile: Boolean = false)

  def main(args: Array[String]): Unit = {
    var master: String = "local"
    var inputFile: String = null
    var compileOnly: Boolean = false

    val parser = new scopt.OptionParser[CompilerConfig]("PigCompiler") {
      head("PigCompiler", "0.1")
      opt[String]('m', "master") optional() action { (x, c) => c.copy(master = x) } text("spark://host:port, mesos://host:port, yarn, or local.")
      opt[Unit]('c', "compile") action { (_, c) => c.copy(compile = true) } text("compile only (don't execute the script)")
      help("help") text("prints this usage text")
      version("version") text("prints this version info")
      arg[String]("<file>") required() action { (x, c) => c.copy(input = x) } text("Pig file")
    }
    // parser.parse returns Option[C]
    parser.parse(args, CompilerConfig()) match {
      case Some(config) => {
        // do stuff
        master = config.master
        inputFile = config.input
        compileOnly = config.compile
      }
      case None =>
      // arguments are bad, error message will have been displayed
        return
    }

      // 1. we read the Pig file
    // val inputFile = args(0)
    val reader = new FileReader(inputFile)
    val source = Source.fromFile(inputFile)
    
    val fileName = new java.io.File(inputFile).getName

    // 2. then we parse it and construct a dataflow plan
    val plan = new DataflowPlan(parseScriptFromSource(source))
    if (! plan.checkSchemaConformance) {
      println("ERROR: schema conformance")
      return
    }

    // 3. now, we should apply optimizations
    val scriptName = fileName.replace(".pig", "")

    // 4. compile it into Scala code for Spark
    val compile = new SparkCompile
    val code = compile.compile(scriptName, plan)

    // 5. write it to a file
    val outputFile = fileName.replace(".pig", ".scala")
    val writer = new FileWriter(outputFile)
    writer.append(code)
    writer.close()

    if (compileOnly) sys.exit(0)

    // 6. compile the Scala code
    val outputDirectory = new java.io.File(".").getCanonicalPath + "/out"

    // check whether output directory exists
    val dirFile = new java.io.File(outputDirectory)
    // if not then create it
    if (!dirFile.exists)
      dirFile.mkdir()

    ScalaCompiler.compile(outputDirectory, outputFile)

    // 7. build a jar file
    val jarFile = scriptName + ".jar"
    JarBuilder.apply(outputDirectory, jarFile, true)

    // 8. and finally call SparkSubmit
    SparkSubmit.main(Array("--master", master, "--class", scriptName, jarFile))
  }
}
