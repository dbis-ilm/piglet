package dbis.pfabric.deploy.config
import com.typesafe.config.ConfigFactory
import dbis.pfabric.deploy.DeployConfig

/**
 * @brief A configuration class to get the yarn cluster configuration for the task. If the values is not set by the
 * user, the default value will be retrieved.
 */
object YarnConfigConstants {
  val TASK_NAME = "yarn.task.name"
  val CONTAINER_MAX_MEMORY = "yarn.container.memory"
  val CONTAINER_MAX_CORES = "yarn.container.cores"
  val CONTAINER_RETRY_COUNT = "yarn.container.retry.count"
  val AM_JMX_ENABLED = "yarn.am.jmx.enabled"
  val AM_CONTAINER_MAX_MEMORY = "yarn.am.container.memory"
  val AM_CONTAINER_MAX_CORES = "yarn.am.container.cores"
  val AM_POLL_INTERVAL = "yarn.am.poll.interval"
}

object YarnConfig extends DeployConfig {
  
  init("default-yarn")
  
  def getContainerMaxMemoryMb: Int = systemConf.getInt(YarnConfigConstants.CONTAINER_MAX_MEMORY)

  def getContainerMaxCpuCores: Int = systemConf.getInt(YarnConfigConstants.CONTAINER_MAX_CORES)

  def getContainerRetryCount: Int = systemConf.getInt(YarnConfigConstants.CONTAINER_RETRY_COUNT)

  def getAMContainerMaxMemoryMb: Int = systemConf.getInt(YarnConfigConstants.AM_CONTAINER_MAX_MEMORY)

  def getAMContainerMaxCpuCores: Int = systemConf.getInt(YarnConfigConstants.AM_CONTAINER_MAX_CORES)

  def getAMPollIntervalMs: Int = systemConf.getInt(YarnConfigConstants.AM_POLL_INTERVAL)

  def getJmxServerEnabled: Boolean = systemConf.getBoolean(YarnConfigConstants.AM_JMX_ENABLED)

  def getDeploySystem: String = "YARN"

  def getTaskName: String = systemConf.getString(YarnConfigConstants.TASK_NAME)
  
  
}
