package dbis.piglet.tools

import java.net.URI
import java.io.File
import java.nio.file.Files
import java.nio.file.{Path,  Paths}

import scala.collection.JavaConverters._

import scopt.OptionParser

import dbis.piglet.BuildInfo
import dbis.piglet.tools.logging.LogLevel
import dbis.piglet.tools.logging.LogLevel._

/**
 * The parameters object that holds all arguments of the command line
 * 
 * @param master Application master (local, yarn, mesos, ...)
 * @param inputFiles A list of pig scripts to execute
 * @param compileOnly Flag to indicate that code just has to be generated and compiled, but not executed
 * @param outDir base directory for writing generated output files to
 * @param params Mapping of parameter names used in the script
 * @param backend The name of the backend to use
 * @param backendPath The path to the library file of the used backend (sparklib, flinklib, ...)
 * @param updateConfig Flag to indicate to just copy the config file to home directory and then stop
 * @param showPlan Print the final plan
 * @param backendArgs Additional arguments/parameters to pass to the backend
 * @param profiling To enable profiling, provide a URI to a server that will retrieve the profiling information
 * @param logLevel Set the log level
 * @param sequential If multiple input files are provided, they're executed sequentially (we won't merge them into a single plan)
 * @param keepFiles Do not delete generated source and class files
 * @param showStats Print runtime statistics after execution
 * @param paramFile A file containing parameter substitutions
 * @param interactive Flag to start Piglet in interactive mode  
 */
case class CliParams(
  master: String = "local",
  inputFiles: Seq[Path] = Seq.empty,
  compileOnly: Boolean = false,
  outDir: Path = Paths.get("."),
  params: Map[String, String] = Map.empty,
  backend: String = Conf.defaultBackend,
  backendPath: Path = Paths.get("."),
  updateConfig: Boolean = false,
  showPlan: Boolean = false,
  backendArgs: Map[String, String] = Map.empty,
  profiling: Option[Path] = None,
  profilingPort: Int = 8000,
  logLevel: LogLevel = LogLevel.WARN,
  sequential: Boolean = false,
  keepFiles: Boolean = false,
  showStats: Boolean = false,
  paramFile: Option[Path] = None,
  interactive: Boolean = false,
  quiet: Boolean = false,
  notifyURL: Option[URI] = None,
  muteConsumer: Boolean = false
) {
  
  require(!showPlan || (showPlan && !quiet), "show-plan and quiet cannot be active at the same time" )
  require(!showStats || (showStats && !quiet), "show-stats and quiet cannot be active at the same time" )
  require(!muteConsumer || (muteConsumer && !interactive), "dev-null cannot be used in interactive mode")
}

object CliParams {

  private lazy val optparser = new OptionParser[CliParams]("Piglet ") {
    head("Piglet", s"ver. ${BuildInfo.version} (built at ${BuildInfo.builtAtString})")
    opt[Unit]('i', "interactive") action { (_, c) => c.copy(interactive = true) } text ("start an interactive REPL")
    opt[String]('m', "master") optional() action { (x, c) => c.copy(master = x) } text ("spark://host:port, mesos://host:port, yarn, or local.")
    opt[String]('b', "backend") optional() action { (x, c) => c.copy(backend = x ) } text ("Target backend (spark, flink, sparks, ...)")
    opt[String]("backend_dir") optional() action { (x, c) => c.copy(backendPath = new File(x).toPath()) } text ("Path to the diretory containing the backend plugins")
    opt[Map[String, String]]('p', "params") valueName ("name1=value1,name2=value2...") action { (x, c) => 
        val prevAndNew = c.params ++ x
        c.copy(params = prevAndNew) 
      } text ("parameter(s) to subsitute")
    opt[File]("param-file") optional() action { (x,c) => 
        var newC = c.copy( paramFile = Some(x.toPath()) )
        

        /*
         * If the parameter file is given, read each line, split it by = and add
         * the mapping to the parameters list
         */
        val fileParams = Files.readAllLines(x.toPath()).asScala
            .map { line => line.split("=", 2) } // 2 tells split to apply the regex 1 time (n-1) - the result array will be of size 2 (n)
            .map { arr => (arr(0) -> arr(1) )}.toMap 
            
        val allParams = fileParams ++ newC.params
        newC = newC.copy(params = allParams)
            
            
        newC
        
      } text ("Path to a file containing parameter value pairs")
    opt[Map[String, String]]("backend-args") valueName ("key1=value1,key2=value2...") action { (x, c) => c.copy(backendArgs = x) } text ("parameter(s) to substitute")
    opt[Unit]('c', "compile") action { (_, c) => c.copy(compileOnly = true) } text ("compile only (don't execute the script)")
    opt[File]('o', "outdir") optional() action { (x, c) => c.copy(outDir = x.toPath()) } text ("output directory for generated code")
    opt[Unit]('k',"keep") optional() action { (x,c) => c.copy(keepFiles = true) } text ("keep generated files")
    opt[String]('g', "log-level") optional() action { (x, c) => c.copy(logLevel = LogLevel.withName(x.toUpperCase())) } text ("Set the log level: DEBUG, INFO, WARN, ERROR")
    opt[Unit]('s', "show-plan") optional() action { (_, c) => c.copy(showPlan = true) } text (s"show the execution plan")
    opt[File]("profiling") optional() action { (x, c) => c.copy(profiling = Some(x.toPath())) } text ("Activate profiling and use given file as storage")
//    opt[Int]("profiling-url") optional() action { (x,c) => c.copy(profilingPort = x) } text ("Port to run profiling server on")
    opt[Unit]("show-stats") optional() action { (_,c) => c.copy(showStats = true) } text ("print detailed timing stats at the end")
    opt[Unit]("sequential") optional() action{ (_,c) => c.copy(sequential = true) } text ("sequential execution (do not merge plans)")
    opt[Unit]('u', "update-config") optional() action { (_, c) => c.copy(updateConfig = true) } text (s"update config file in program home (see config file)")
    opt[Unit]('q',"quiet") optional() action { (_,c) => c.copy(quiet = true) } text ("Don't print header output (does not affect logging and error output)")
    opt[URI]('n',"notify") optional() action { (x,c) => c.copy(notifyURL = Some(x)) } text ("URL to call upon exit (in case of error and success). Available placeholders are [time] (time when program finished) , [name] (script name) and [success] (indicator of success or exception)")
    opt[Unit]("mute-consumer") optional() action { (_,c) => c.copy(muteConsumer = true)} text ("Make DUMP or STORE operators consume but NOT write any output")
    help("help") text ("prints this usage text")
    version("version") text ("prints this version info")
    arg[File]("<file>...") unbounded() optional() action { (x, c) => c.copy(inputFiles = c.inputFiles :+ x.toPath()) } text ("Pig script files to execute")
  }
  
  def parse(args: Array[String]): CliParams = optparser.parse(args, CliParams()).getOrElse{
    System.exit(1)
    CliParams() // we won't get here. This is just to satisfy the return type.
  }
  
}
