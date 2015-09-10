package dbis.pig.tools

import org.apache.commons.exec._
import org.apache.commons.exec.environment.EnvironmentUtils
import com.typesafe.config.ConfigFactory
import dbis.pig.backends.CppConfig
import com.typesafe.scalalogging.LazyLogging

object CppCompilerConf extends LazyLogging {
 
  /**
   * Get the config class for the c++ compiler with the given backend
   * 
   * @param backend The name of the backend. 
   * @return Returns a new instance of the compiler config class
   */
  def cppConf(backend: String): CppConfig = {
    
    val className = Conf.backendCompileConf(backend)
    
    logger.debug(s"""loading cpp config for backend "$backend" with name: $className""")
    
    Class.forName(className).newInstance().asInstanceOf[CppConfig]
  }
}

object CppCompiler {
  def compile (targetDir: String, sourceFile: String, cppConf: CppConfig, verbose: Boolean = true) : Boolean = {
    val map = new java.util.HashMap[String, Object]()
    map.put("file", new java.io.File(sourceFile))
    map.put("exec", sourceFile.substring(0, sourceFile.lastIndexOf(".cpp")))
    val cmdLine = new CommandLine(cppConf.getCompiler)

    cmdLine.addArgument("-o")
    cmdLine.addArgument("${exec}")
    cmdLine.addArgument("${file}")

   
    cmdLine.setSubstitutionMap(map)
  
    for (s <- cppConf.getOptions) { cmdLine.addArgument("-"+ s) }
    for (s <- cppConf.getIncludeDirs) { cmdLine.addArgument("-I" + s) }
    for (s <- cppConf.getLibDirs) { cmdLine.addArgument("-L"+ s) }
    for (s <- cppConf.getLibraries) { cmdLine.addArgument("-l"+ s) }
    //for (s <- cppConf.getProperty("cpp_includes").split("\\s+")) { cmdLine.addArgument(s) }

    val executor = new DefaultExecutor()
    executor.setExitValue(0)
    val watchdog = new ExecuteWatchdog(120000)
    executor.setWatchdog(watchdog)
    if (verbose)
      println("EXECUTE: " + cmdLine)
    executor.execute(cmdLine) == 0
  }
}