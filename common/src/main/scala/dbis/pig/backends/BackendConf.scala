package dbis.pig.backends

/**
 * @author hage
 */
trait BackendConf {
  /**
   * Get the name of this backend
   *
   * @return Returns the name of this backend
   */
  def name: String
  
  /**
   * Get an instance of runner that will be used to run the jobs 
   *
   * @return Returns the full qualified name of the runner class
   */
  def runnerClass: PigletBackend
  
  /**
   * Get the full path to the template file to use for the backend
   *
   * @return the name of the template file
   */
  def templateFile: String

  /**
   * Get the default connection function used for source and sink nodes
   *
   * @return the name of the function
   */
  def defaultConnector: String

}
