package dbis.pig.backends

import dbis.pig.tools.Conf
import dbis.pig.tools.logging.PigletLogging
import java.net.URLClassLoader

case class NotInitializedException(msg: String) extends Exception(msg)

case class AlreadyInitializedException(msg: String) extends Exception(msg)

/**
 * Manages the available backends. 
 */
object BackendManager extends PigletLogging {
 
  /**
   * Loaded backend
   */
  private var _backend: BackendConf = _

  /**
   * Check if the BackendManager has already been initialized for a backend
   * 
   * @see [[BackendManager.init]]
   */
  def isInitialized = _backend != null
  
  /**
   * Get the current backend configuration
   * 
   * @throws [[NotInitializedException]] if BackendManager has not been initialized yet.
   */
  def backend = if(isInitialized) _backend else throw NotInitializedException("BackendManager not initialized yet")


  /**
   * Initialized the manager for the current backend, that is, load the [[BackendConf]] from the FQN
   * as given in Piglet's config file. 
   * 
   * 
   * @param backend The name of the backend. 
   * @return Returns a new instance of the [[BakcendConf]] (whose FQN was specified in Piglet config file)
   */
  def init(backendName: String): BackendConf = {
    
    if(isInitialized)
      logger.info(s"BackendManager already initialized as ${backend.toString()}")
//      throw AlreadyInitializedException(s"BackendManager already initialized as ${backend.toString()}")
    
    val className = Conf.backendConf(backendName)
    
    logger.debug(s"""loading runner class for backend "$backendName" with name: $className""")
    
    _backend = Class.forName(className).newInstance().asInstanceOf[BackendConf]
    
    backend
  }

}
