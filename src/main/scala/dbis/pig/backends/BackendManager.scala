package dbis.pig.backends

import dbis.pig.tools.Conf


/**
 * @author hage
 */
object BackendManager {
 
  def backendConf(backend: String): PigletBackend = {
    val className = Conf.backendConf(backend)
    
    Class.forName(className).newInstance().asInstanceOf[PigletBackend]
  }
  
}