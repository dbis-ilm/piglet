package dbis.pfabric.deploy.yarn.appmaster

import scala.collection._
import scala.collection.convert.decorateAsScala._
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions.asScalaBuffer
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{ Container, ContainerStatus, NodeReport }
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.client.api.TimelineClient
import dbis.pfabric.deploy.config._
import dbis.pfabric.deploy.yarn.client.YarnRMClient
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import dbis.pfabric.deploy.yarn.client._
import dbis.pfabric.deploy.util.HDFSUtils
import org.apache.hadoop.fs.FileSystem

object AMValidationUtils {
  def validationMessage(mem: Int, cores: Int): String = s"The YARN cluster is unable to execute your job because of the request resource requirements.You asked for memory: ${mem}, and cores: ${cores}."
}

object AppMaster extends AMRMClientAsync.CallbackHandler with LazyLogging {
  var listeners: List[AppMasterListener] = null
  val conf = new YarnConfiguration()
  var state: AMState = null
  def main(args: Array[String]) {
    logger.info("Starting ApplicationMaster");

    val envs = System.getenv().asScala
    envs.foreach { case (key, value) => logger.info(key + "-->" + value) }
      
    val containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString)
    logger.info(s"got container id: $containerIdStr")
    val containerId = ConverterUtils.toContainerId(containerIdStr)
    val applicationAttemptId = containerId.getApplicationAttemptId
    logger.info(s"got application attempt id: $applicationAttemptId")
    val nodeHostString = System.getenv(ApplicationConstants.Environment.NM_HOST.toString)
    logger.info(s"got node manager host: $nodeHostString")
    val nodePortString = System.getenv(ApplicationConstants.Environment.NM_PORT.toString)
    logger.info(s"got node manager port: $nodePortString")
    val nodeHttpPortString = System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.toString)
    logger.info(s"got node manager http port: $nodeHttpPortString")

    val interval = YarnConfig.getAMPollIntervalMs
    val amClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](interval, this)
    val containerMem = YarnConfig.getContainerMaxMemoryMb
    val containerCores = YarnConfig.getContainerMaxCpuCores

    val timelineClient = TimelineClient.createTimelineClient();
    timelineClient.init(conf);
    timelineClient.start();
    
    val fs: FileSystem = FileSystem.get(conf)
    state = new AMState(-1, containerId, nodeHostString, nodePortString.toInt, nodeHttpPortString.toInt)
    envs.get(YarnTask.TASKS_FOLDER) match {
      case Some(t) => {
         val tasks = HDFSUtils.getFileList(fs, t)
         logger.info(tasks.mkString)
          state.tasks = tasks
          state.containerCount = tasks.length
          state.requestContainers = (0 to state.containerCount).toSet
      }
      case None =>  throw new RuntimeException( " tasks folder is not set in the environment");
    }
   
    amClient.init(conf)
    amClient.start
    val response = amClient.registerApplicationMaster("", -1, "") // TODO
    
    // check if the YARN cluster can satisfy our container resource requirements
    val maxCapability = response.getMaximumResourceCapability
    val maxMem = maxCapability.getMemory
    val maxCores = maxCapability.getVirtualCores

    logger.info(s"Got AM register response. The YARN RM supports container requests with maximum memory: ${maxMem} and maximum cores: ${maxCores}")

    if (containerMem > maxMem || containerCores > maxCores) {
      logger.error(AMValidationUtils.validationMessage(containerMem, containerCores))
      state.status = FinalApplicationStatus.FAILED
      state.jobHealthy = false
      amClient.unregisterApplicationMaster(FinalApplicationStatus.FAILED, AMValidationUtils.validationMessage(containerMem, containerCores), null)
    } else {
      val amManager = new AMJobManager({ System.currentTimeMillis }, state, amClient, conf)
      listeners = List(amManager)
      runAM(amClient, listeners, conf, interval)
    }
  }

  def runAM(amClient: AMRMClientAsync[ContainerRequest], listeners: List[AppMasterListener], hConfig: YarnConfiguration, interval: Int): Unit = {

    try {
      listeners.foreach(_.onInit)
      var isShutdown: Boolean = false
      //to prevent the process from exiting until either the job is shutdown or error occurs on the application master
      while (!isShutdown && !listeners.map(_.shouldShutdown).reduceLeft(_ || _)) {
        try {
          Thread.sleep(interval)
        } catch {
          case e: InterruptedException => {
            isShutdown = true
            logger.info("got interrupt in app master thread, so shutting down ... ")
          }
        }
      }
    } finally {
      // listeners has to be stopped
      listeners.foreach(listener => try {
        listener.onShutdown
      } catch {
        case e: Exception => logger.warn(s"Listener ${listener} failed to shutdown.", e)
      })
      // amClient has to be stopped
      amClient.unregisterApplicationMaster(state.status, null, null)
      amClient.stop
    }
  }

  override def onContainersCompleted(statuses: java.util.List[ContainerStatus]): Unit =
    statuses.foreach(containerStatus => listeners.foreach(_.onContainerCompleted(containerStatus)))

  override def onShutdownRequest: Unit = listeners.foreach(_.onReboot)

  override def onNodesUpdated(updatedNodes: java.util.List[NodeReport]): Unit = Unit

  override def onContainersAllocated(containers: java.util.List[Container]): Unit =
    containers.foreach(container => listeners.foreach(_.onContainerAllocated(container)))

  override def onError(e: Throwable): Unit = {
    logger.error("Error occured in application master callback", e)
  }

  override def getProgress: Float = state.completedContainers / state.containerCount

}