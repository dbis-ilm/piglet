package dbis.pfabric.deploy.config

  import com.typesafe.config.ConfigFactory
import dbis.pfabric.deploy.DeployConfig

/**
 * @brief A configuration class to get the Mesos cluster configuration for the task. If the values is not set by the
 * user, the default value will be retrieved.
 */
object MesosConfigConstants {
  val TASK_NAME = "mesos.task.name"
  val ENABLE_CHECKPOINT = "mesos.checkpoint"
  val JOB_MAX_MEMORY = "mesos.job.memory"
  val JOB_MAX_CORES = "mesos.job.cores"
  val MESOS_MASTER = "mesos.master"
  val EXCUTOR_LOCATION = "mesos.executor"
}

object MesosConfig extends DeployConfig {
  
  init("default-mesos")
  
  def getDeploySystem: String = "Mesos"

  def getJobMaxMemoryMb: Int = systemConf.getInt(MesosConfigConstants.JOB_MAX_MEMORY)

  def getJobMaxCpuCores: Int = systemConf.getInt(MesosConfigConstants.JOB_MAX_CORES)

  def getTaskName: String = systemConf.getString(MesosConfigConstants.TASK_NAME)

  def getCheckPointEnable: Boolean = systemConf.getBoolean(MesosConfigConstants.ENABLE_CHECKPOINT)

  def getMaster: String = systemConf.getString(MesosConfigConstants.MESOS_MASTER)

  def getExecutorLocation: String = systemConf.getString(MesosConfigConstants.EXCUTOR_LOCATION)
  
}
