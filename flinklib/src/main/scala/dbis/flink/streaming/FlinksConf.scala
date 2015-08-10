package dbis.flink.streaming

import dbis.pig.backends.BackendConf
import dbis.flink.FlinkRun
import dbis.pig.backends.PigletBackend
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
}