package dbis.piglet.backends.flink.streaming

import dbis.piglet.backends.BackendConf
import dbis.piglet.backends.flink.FlinkRun
import dbis.piglet.backends.PigletBackend
import com.typesafe.config.ConfigFactory

/**
 * @author hage
 */
class FlinksConf extends BackendConf {
  // loads the default configuration file in resources/application.conf
  private val appconf = ConfigFactory.load()
  
  /**
   * Get the name of this backend
   * 
   * @return Returns the name of this backend
   */
  override def name: String = appconf.getString("backends.flinks.name")
  
  /**
   * Get the path to the runner class that implements the PigletBackend interface
   */
  override def runnerClass: PigletBackend = {
    new FlinkRun
  } 
  
  override def templateFile: String = appconf.getString("backends.flinks.template")

  override def defaultConnector: String = appconf.getString("backends.flinks.connector")
  
  override def raw = false
}
