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
   * Defines that a backends needs the raw Pig script
   * rather than the generated code
   * 
   * @return <code>True</code> if the backends wants the original script, otherwise <code>false</code> 
   */
  def raw: Boolean
}