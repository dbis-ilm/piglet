package dbis.pig.tools

import com.typesafe.config.ConfigFactory
import java.io.File

object Conf {
  
  // loads the default configuration file in resources/application.conf
  private val appconf = ConfigFactory.load()
  
  
  def materializationBaseDir: File = new File(appconf.getString("materialization.basedir"))
  def materializationMapFile: File = new File(materializationBaseDir, 
                                                appconf.getString("materialization.mapfile"))
 
  
  def flinkBackendJar: String = appconf.getString("backends.flink.jar")
  def sparkBackendJar: String = appconf.getString("backends.spark.jar")
  
}