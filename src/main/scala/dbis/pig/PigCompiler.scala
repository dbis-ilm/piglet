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

  def main(args: Array[String]): Unit = {
    // 1. we read the Pig file
    val reader = new FileReader(args(0))
    val source = Source.fromFile(args(0))

    // 2. then we parse it and construct a dataflow plan
    val plan = new DataflowPlan(parseScriptFromSource(source))

    // 3. now, we should apply optimizations
    val scriptName = args(0).replace(".pig", "")

    // 4. compile it into Scala code for Spark
    val compile = new SparkCompile
    val code = compile.compile(scriptName, plan)

    // 5. write it to a file
    val outputFile = args(0).replace(".pig", ".scala")
    val writer = new FileWriter(outputFile)
    writer.append(code)
    writer.close()

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
    SparkSubmit.main(Array("--master", "local", "--class", scriptName, jarFile))
  }
}
