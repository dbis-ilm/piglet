package dbis.flink

import dbis.pig.backends.BackendConf
import com.typesafe.config.ConfigFactory
import dbis.pig.backends.PigletBackend

/**
 * @author hage
 */
class FlinkConf extends BackendConf {
  
  // loads the default configuration file in resources/application.conf
  private val appconf = ConfigFactory.load()
  
  /**
   * Get the name of this backend
   * 
   * @return Returns the name of this backend
   */
  override def name: String = appconf.getString("backends.flink.name")
  
  /**
   * Get the path to the runner class that implements the PigletBackend interface
   */
  override def runnerClass: PigletBackend = {
    new FlinkRun
  } 
  
  override def templateFile: String = appconf.getString("backends.flink.template")

  override def defaultConnector: String = appconf.getString("backends.flink.connector")
}
