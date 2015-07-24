package dbis.pig.backends

import dbis.pig.tools.Conf
import com.typesafe.scalalogging.LazyLogging
import java.net.URLClassLoader


/**
 * Manages the available backends. 
 */
object BackendManager extends LazyLogging {
 
  /**
   * Get the runner class for the backend with the given name
   * 
   * @param backend The name of the backend. 
   * @return Returns a new instance of the runner class (whose FQN was specified in Piglet config file)
   */
  def backend(backend: String): BackendConf = {
    
    val className = Conf.backendConf(backend)
    
    logger.debug(s"""loading runner class for backend "$backend" with name: $className""")
    
    Class.forName(className).newInstance().asInstanceOf[BackendConf]
  }
}