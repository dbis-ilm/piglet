package dbis.pig.backends

import dbis.pig.tools.Conf
import com.typesafe.scalalogging.LazyLogging


/**
 * @author hage
 */
object BackendManager extends LazyLogging {
 
  def backendConf(backend: String): PigletBackend = {
    val className = Conf.backendConf(backend)
    
    logger.debug(s"loading runner class with name: $className")
    
    Class.forName(className).newInstance().asInstanceOf[PigletBackend]
  }
  
}