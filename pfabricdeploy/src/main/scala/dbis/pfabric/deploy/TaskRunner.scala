package dbis.pfabric.deploy

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import java.net.URI

/**
 * An enumeration is used by our system  to indicate the current status of the deployed task
 */
object TaskStatus extends Enumeration {
  type TaskStatus = Value
  val Running, Successful, Unsuccessful, New = Value
}

/**
 * An exception indicating failures in the deployment process.
 *
 * @param msg a message describing the exception.
 */
case class DeployException(private val msg: String) extends Exception(msg)

/**
 * A DeployTask runs a PieFabric task in its specific computing environment (cluster management technology). This environment
 * can be YARN, Local, or even Mesos. In general, this class, and its accompanying factory,
 * should be extended to allow PieFabric task to be run on different service providers such as YARN, Mesos, etc.
 */
import TaskStatus._

trait TaskRunner {
  /**
   * Submit the PipeFabric task to be run on the specific environment
   * @return The task after it has been submitted.
   */
  def submit(): TaskRunner

  /**
   * Kill this Task immediately.
   *
   * @return The task after it has been killed.
   */
  def kill(): TaskRunner

  /**
   * wait the task until either it finishes or reaches its timeout value
   *
   * @param timeout how many milliseconds to wait for the task in case that it has not been finished
   * @return the task status after finishing or timing out
   */
  def waitForFinish(timeout: Long): TaskStatus

  /**
   * wait the task until either it reaches the specified status or reaches it timeout value
   *
   * @param status the target status to wait upon
   * @param timeout  how many milliseconds to wait for the task in case that it has not been reached the specified status
   * @return the task status after finishing or reaching target state
   */
  def waitForStatus(status: TaskStatus, timeout: Long): TaskStatus

  /**
   * Get current status of the Task
   * @return Current task status
   */
  def getStatus(): TaskStatus
}

/**
 * A configuration for deployment
 */
trait DeployConfig {
  private var defaultConfig: String = null
  protected var systemConf: Config = null

  /**
   * specify the cluster management system or the computing environment
   */
  def getDeploySystem(): String
  /**
   * get the task name
   */
  def getTaskName(): String

  def init(config: String): Unit = {
    defaultConfig = config
    systemConf = ConfigFactory.load().withFallback(ConfigFactory.load(defaultConfig))
  }
  
  def invalidateCache: Unit = { 
    ConfigFactory.invalidateCaches 
    init(defaultConfig)
  }

}

/**
 * Build the complete task to be deployed in the cluster
 */
trait TaskRunnerFactory {
  /**
   * get the task
   */
  def getTaskRunner(task: URI, supportingFile: URI, numberOfTasks: Int): TaskRunner
}