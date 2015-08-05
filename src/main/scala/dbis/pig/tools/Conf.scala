package dbis.pig.tools

import com.typesafe.config.ConfigFactory
import java.io.File
import java.net.URI

object Conf {
  
  // loads the default configuration file in resources/application.conf
  private val appconf = ConfigFactory.load()
  
  
  def materializationBaseDir = new URI(appconf.getString("materialization.basedir"))
  def materializationMapFile = new File(appconf.getString("materialization.mapfile")).toPath()
 
  
  def backendJar(backend: String) = appconf.getString(s"backends.$backend.jar")
}