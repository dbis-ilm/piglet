package dbis.pig.tools

import com.typesafe.config.ConfigFactory
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths

object Conf {
  
  // loads the default configuration file in resources/application.conf
  private val appconf = ConfigFactory.load()
  
  
  def materializationBaseDir: File = new File(appconf.getString("materialization.basedir"))
  def materializationMapFile: File = new File(materializationBaseDir, 
                                                appconf.getString("materialization.mapfile"))
 
  
  def defaultBackend = appconf.getString("backends.default")
  
  def backendJar(backend: String): Path = Paths.get(appconf.getString(s"backends.$backend.jar")) 
  
  def backendConf(backend: String) = appconf.getString(s"backends.$backend.conf")
}