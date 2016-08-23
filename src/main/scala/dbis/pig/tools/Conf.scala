package dbis.pig.tools

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.io.File
import java.net.URI
import java.nio.file.Paths
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.Path
import scala.collection.JavaConverters._

import dbis.pig.tools.logging.PigletLogging
import java.nio.file.attribute.FileAttribute

/**
 * This is the global configuration object that contains all user-defined values
 */
object Conf extends PigletLogging {
  
  val EXECTIMES_FRAGMENT = "exectimes"
  val MATERIALIZATION_FRAGMENT = "materializations"
  
  
	val programHome = Paths.get(System.getProperty("user.home"), ".piglet")
	
	// create directory if not exists
	if(!Files.exists(programHome))
		Files.createDirectory(programHome)
  
  /**
   * The path to the config file. It will resolve to $USER_HOME/.piglet/piglet.conf
   */
  private val configFile = "piglet.conf"
  
  /**
   * Load the configuration.
   * 
   * This loads the configuration from the user's home directory. If the config file cannot be found
   * (see [[Conf#configFile]]) the default values found in src/main/resources/piglet.conf are
   * copied to [[Conf#configFile]]
   * 
   * @return Returns the config object
   */
  private def loadConf: Config = {
    
	  val userConf = programHome.resolve(configFile)
	  
    if(Files.exists(userConf)) {
      
      // if the config file exists in the program home, use this one
      logger.info(s"using $userConf as config file")
      ConfigFactory.parseFile(userConf.toFile())
      
    } else {
      // Otherwise, use the packaged one 
      logger.info(s"loading default packaged config file")
      ConfigFactory.load(configFile)
    }
  }

  protected[pig] def copyConfigFile() = {
    val source = Conf.getClass.getClassLoader.getResourceAsStream(configFile)
    val dest = programHome.resolve(configFile)
    
    if(Files.exists(dest)) {
      val bak = new File(s"${dest.toAbsolutePath().toString()}.bak").toPath()
      logger.debug(s"create bakup file as $bak")
      
      Files.copy(dest, bak, StandardCopyOption.REPLACE_EXISTING)
    }
    
    Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING)
    logger.debug(s"copied config file to $dest")
  }
  
  // loads the configuration file 
  private lazy val appconf = loadConf
  
  def replHistoryFile = programHome.resolve(appconf.getString("repl.history"))  

  def materializationBaseDir = new URI(appconf.getString("materialization.basedir"))
  def materializationMapFile = Paths.get(appconf.getString("materialization.mapfile")).toAbsolutePath()
 
  
  def defaultBackend = appconf.getString("backends.default")
  
  def backendJar(backend: String): Path = Paths.get(appconf.getString(s"backends.$backend.jar")) 
  
  def backendConf(backend: String) = appconf.getString(s"backends.$backend.conf")
  
  // TODO: shouldn't we move this to the backend config? (not in Piglet itself)
  def backendGenerator(backend: String) = appconf.getString(s"backends.$backend.generator.class")
  def backendExtension(backend: String) = appconf.getString(s"backends.$backend.generator.extension")
  def backendCompileConf(backend: String) = appconf.getString(s"backends.$backend.compileconf")
  
  def hdfsCoreSiteFile = Paths.get(appconf.getString("hdfs.coresite"))
  def hdfsHdfsSiteFile = Paths.get(appconf.getString("hdfs.hdfssite"))
  
  
//  def langfeatureImports(feature: String) = appconf.getStringList(s"langfeature.$feature.imports").asScala
//  def langfeatureAdditionalJars(feature: String) = appconf.getStringList(s"langfeature.$feature.jars")
  
}