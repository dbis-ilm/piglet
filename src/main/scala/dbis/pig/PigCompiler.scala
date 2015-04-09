package dbis.pig

import java.io.{FileReader, FileWriter}
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
    val reader = new FileReader(args(0))
    val source = Source.fromFile(args(0))
    val plan = new DataflowPlan(parseScriptFromSource(source))

    val scriptName = args(0).replace(".pig", "")
    val compile = new SparkCompile
    val code = compile.compile(scriptName, plan)

    val outputFile = args(0).replace(".pig", ".scala")
    val writer = new FileWriter(outputFile)
    writer.append(code)
    writer.close()
  }
}
