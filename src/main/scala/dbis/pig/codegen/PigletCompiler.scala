package dbis.pig.codegen

import dbis.pig.plan.DataflowPlan
import dbis.pig.parser.LanguageFeature
import java.nio.file.Path
import com.typesafe.scalalogging.LazyLogging
import dbis.pig.parser.PigParser
import scala.io.Source
import scala.collection.mutable.ListBuffer
import dbis.pig.parser.PigParser
import dbis.pig.op.PigOperator
import java.nio.file.Paths

object PigletCompiler extends LazyLogging {
  
  /**
   * Helper method to parse the given file into a dataflow plan
   * 
   * @param inputFile The file to parse
   * @param params Key value pairs to replace placeholders in the script
   * @param backend The name of the backend
   */
  def createDataflowPlan(inputFile: Path, params: Map[String,String], backend: String, langFeature: LanguageFeature.LanguageFeature): Option[DataflowPlan] = {
      // 1. we read the Pig file
      val source = Source.fromFile(inputFile.toFile())
      
      logger.debug(s"""loaded pig script from "$inputFile" """)
  
      // 2. then we parse it and construct a dataflow plan
      val plan = new DataflowPlan(parseScriptFromSource(source, params, backend, langFeature))
      

      if (!plan.checkConnectivity) {
        logger.error(s"dataflow plan not connected for $inputFile")
        return None
      }

      logger.debug(s"successfully created dataflow plan for $inputFile")

      return Some(plan)
    
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
   def resolveImports(lines: Iterator[String]): Iterator[String] = {
    val buf = ListBuffer.empty[String]
    for (l <- lines) {
      if (l.matches("""[ \t]*[iI][mM][pP][oO][rR][tT][ \t]*'([^'\p{Cntrl}\\]|\\[\\"bfnrt]|\\u[a-fA-F0-9]{4})*'[ \t\n]*;""")) {
        val s = l.split(" ")(1)
        val name = s.substring(1, s.length - 2)
        val path = Paths.get(name)
        val resolvedLine = resolveImports(loadScript(path))
        buf ++= resolvedLine
      }
      else
        buf += l
    }
    buf.toIterator
  }
   
  private def loadScript(inputFile: Path): Iterator[String] = Source.fromFile(inputFile.toFile()).getLines()
  
  private def parseScriptFromSource(source: Source, params: Map[String,String], backend: String, langFeature: LanguageFeature.LanguageFeature): List[PigOperator] = {
	  
    //Handle IMPORT statements.
	  val sourceLines = resolveImports(source.getLines())
	  
	  if (params.nonEmpty) {

	    // Replace placeholders by parameters.
		  PigParser.parseScript(sourceLines.map(line => replaceParameters(line, params)).mkString("\n"), langFeature)
	  }
	  else {
		  PigParser.parseScript(sourceLines.mkString("\n"), langFeature)
	  }
  }
  
}