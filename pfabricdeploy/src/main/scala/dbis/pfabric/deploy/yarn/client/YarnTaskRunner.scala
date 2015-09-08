package dbis.pfabric.deploy.yarn.client

import dbis.pfabric.deploy._
import dbis.pfabric.deploy.util.YarnUtils
import dbis.pfabric.deploy.util.HDFSUtils
import dbis.pfabric.deploy.TaskStatus._
import dbis.pfabric.deploy.config.YarnConfig
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.conf.YarnConfiguration
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.LocalResource
import scala.collection.mutable.Map
import java.net.URI

object YarnTask {
  val APPLICATION_MASTER_JAR = "deploy.jar"
  val TASKS_FOLDER = "TASK_FOLDER"
}
/**
 * A factory for the yarn task
 */
object YarnDeployTaskFactory extends TaskRunnerFactory {
  /**
   * get the generated task to the system
   */
  override def getTaskRunner(task: URI, supportingFile: URI, numberOfTasks: Int) = {
    val yConfig = new YarnConfiguration
    new YarnTaskRunner(yConfig, task.getPath, supportingFile.getPath, numberOfTasks)
  }
}
/**
 *
 * A class represents the actual task to be executed in the cluster. laster the client will submit this task to the YARN ResourceManager (RM)
 * and monitor the task. From the configuration, the requested command, local resources such as the Application Master jar, and the enviroment
 * would be prepared.
 * @param config the task configuration such as memory and core  requirements as well as its name and path
 * @param yarnConfig the default yarn configuration
 *
 */
class YarnTaskRunner(yarnConfig: YarnConfiguration, task: String, jarFile: String, numberOfTasks: Int) extends TaskRunner with LazyLogging {

  /**
   * a client to submit the task to the YARN ResourceManager (RM)
   */

  var appId: Option[ApplicationId] = None
  var localResources = Map[String, LocalResource]()
  var env = Map[String, String]()

  val client = new YarnRMClient(yarnConfig)
  val fs: FileSystem = FileSystem.get(yarnConfig)
  // move the files from local storage to the HDFS to be used by different
  // nodes in the cluster
  val suffix: String = "/" + YarnConfig.getTaskName() + "/"
  
  HDFSUtils.deleteSystemFolder(fs, suffix)
  
  HDFSUtils.getEnv(fs, suffix, YarnTask.TASKS_FOLDER, task, env)
  // move the ApplicationMaster jar to HDFS
  HDFSUtils.getLocalResource(fs, suffix, YarnTask.APPLICATION_MASTER_JAR, jarFile, localResources)


  def submit: YarnTaskRunner = {

    appId = client.submitApp(
      Some(YarnConfig.getTaskName),
      YarnConfig.getAMContainerMaxMemoryMb,
      YarnConfig.getAMContainerMaxCpuCores,
      localResources,
      List(YarnUtils.prepareCommand(YarnConfig.getTaskName)),
      env ++ YarnUtils.prepareOSEnv(yarnConfig))
    this
  }

  def waitForFinish(timeoutMs: Long): TaskStatus = {
    val startTimeMs = System.currentTimeMillis()

    while (System.currentTimeMillis() - startTimeMs < timeoutMs) {
      Option(getStatus) match {
        case Some(s) => if (Successful.equals(s) || Unsuccessful.equals(s)) return s
        case None    => null
      }

      Thread.sleep(1000)
    }

    Running
  }

  def waitForStatus(status: TaskStatus, timeoutMs: Long): TaskStatus = {
    val startTimeMs = System.currentTimeMillis()

    while (System.currentTimeMillis() - startTimeMs < timeoutMs) {
      Option(getStatus) match {
        case Some(s) => if (status.equals(s)) return status
        case None    => null
      }
      Thread.sleep(1000)
    }

    Running
  }
  def getStatus: TaskStatus = {
    appId match {
      case Some(appId) => client.status(appId).getOrElse(null)
      case None        => null
    }
  }

  def kill: YarnTaskRunner = {
    appId match {
      case Some(appId) => client.kill(appId)
      case None        => None
    }
    this
  }
}