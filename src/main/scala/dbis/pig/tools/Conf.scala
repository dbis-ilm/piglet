package dbis.pig.tools

import com.typesafe.config.ConfigFactory
import java.io.File
import java.net.URI
import com.typesafe.scalalogging.LazyLogging
import java.nio.file.Paths
import com.typesafe.config.Config
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.Path

/**
 * This is the global configuration object that contains all user-defined values
 */
object Conf extends LazyLogging {
  
	val programHome = Paths.get(System.getProperty("user.home"), ".piglet")
  
  /**
   * The path to the config file. It will resolve to $USER_HOME/.piglet/application.conf
   */
  private val configFile = programHome.resolve("application.conf")
  
  /**
   * Load the configuration.
   * 
   * This loads the configuration from the user's home directory. If the config file cannot be found
   * (see [[Conf#configFile]]) the default values found in src/main/resources/application.conf are
   * copied to [[Conf#configFile]]
   * 
   * @return Returns the config object
   */
  private def loadConf: Config = {
    
    // 1. check if the config file in the user's home directory exists
    if(!Files.exists(configFile)) {
      
      // 2. if not, create parent directory if necessary
      if(!Files.exists(configFile.getParent)) {
        Files.createDirectories(configFile.getParent)
        logger.info(s"""created program directory at ${configFile.getParent}""")
      }
      
      // 3. copy config file
      copyConfigFile()
    }
  
    // 4. parse the newly created config file
    ConfigFactory.parseFile(configFile.toFile())
  }

  protected[pig] def copyConfigFile() = {
    val source = Conf.getClass.getClassLoader.getResourceAsStream("application.conf")
    Files.copy(source, configFile, StandardCopyOption.REPLACE_EXISTING)
    logger.debug(s"copied config file to $configFile")
  }
  
  // loads the configuration file 
  private val appconf = loadConf
  
  def replHistoryFile = programHome.resolve(appconf.getString("repl.history"))  

  def materializationBaseDir = new URI(appconf.getString("materialization.basedir"))
  def materializationMapFile = Paths.get(appconf.getString("materialization.mapfile")).toAbsolutePath()
 
  
  def defaultBackend = appconf.getString("backends.default")
  
  def backendJar(backend: String): Path = Paths.get(appconf.getString(s"backends.$backend.jar")) 
  
  def backendConf(backend: String) = appconf.getString(s"backends.$backend.conf")
  
  def hdfsCoreSiteFile = Paths.get(appconf.getString("hdfs.coresite"))
  def hdfsHdfsSiteFile = Paths.get(appconf.getString("hdfs.hdfssite"))
}