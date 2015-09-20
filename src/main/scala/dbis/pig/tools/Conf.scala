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
  private val configFile = "piglet.conf"
  
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
    
	  val userConf = programHome.resolve(configFile)
	  
    if(Files.exists(userConf)) {
      
      // if the config file exists in the program home, use this one
      logger.debug(s"using $userConf as config file")
      ConfigFactory.parseFile(userConf.toFile())
      
    } else {
      // Otherwise, use the packaged one 
      logger.debug(s"loading default packaged config file")
      ConfigFactory.load(configFile)
    }
  }

  protected[pig] def copyConfigFile() = {
    val source = Conf.getClass.getClassLoader.getResourceAsStream(configFile)
    val dest = programHome.resolve(configFile)
    Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING)
    logger.debug(s"copied config file to $dest")
  }
  
  // loads the configuration file 
  private val appconf = loadConf
  
  def replHistoryFile = programHome.resolve(appconf.getString("repl.history"))  

  def materializationBaseDir = new URI(appconf.getString("materialization.basedir"))
  def materializationMapFile = Paths.get(appconf.getString("materialization.mapfile")).toAbsolutePath()
 
  
  def defaultBackend = appconf.getString("backends.default")
  
  def backendJar(backend: String): Path = Paths.get(appconf.getString(s"backends.$backend.jar")) 
  
  def backendConf(backend: String) = appconf.getString(s"backends.$backend.conf")
  
  def backendGenerator(backend: String) = appconf.getString(s"backends.$backend.generator.class")
  def backendExtension(backend: String) = appconf.getString(s"backends.$backend.generator.extension")
  def backendCompileConf(backend: String) = appconf.getString(s"backends.$backend.compileconf")
  
  def hdfsCoreSiteFile = Paths.get(appconf.getString("hdfs.coresite"))
  def hdfsHdfsSiteFile = Paths.get(appconf.getString("hdfs.hdfssite"))
  
  def databaseSetting: ConnectionSetting = {
    val driver = appconf.getString("db.driver")
    val url = appconf.getString("db.url")
    val user = appconf.getString("db.user")
    val pw = appconf.getString("db.password")
    
    ConnectionSetting(driver, url, user, pw)
    
  }

  def hookImport = appconf.getString("hooks.import")
  
}