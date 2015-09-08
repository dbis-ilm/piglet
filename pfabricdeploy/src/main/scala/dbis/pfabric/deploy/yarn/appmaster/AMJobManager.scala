package dbis.pfabric.deploy.yarn.appmaster

import java.nio.ByteBuffer
import java.util.Collections
import scala.collection.JavaConversions._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.NMClient
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.util.Records
import com.typesafe.scalalogging.LazyLogging
import dbis.pfabric.deploy.config._
import org.apache.hadoop.fs.FileSystem
import dbis.pfabric.deploy.util.HDFSUtils

case class ContainerFailure(val count: Int, val lastFailure: Long)

/**
 * AMJobManager is responsible for requesting new containers, handling failures, and notifying the application master that the
 * job is done.
 */
class AMJobManager(clock: () => Long, state: AMState, amClient: AMRMClientAsync[ContainerRequest], conf: YarnConfiguration) extends AppMasterListener with LazyLogging {

  var containerFailures = Map[Int, ContainerFailure]()
  var manyFailedContainers = false
  var containerManager: NMClient = null
  val fs: FileSystem = FileSystem.get(conf)

  override def shouldShutdown = (state.completedContainers == state.containerCount) || manyFailedContainers

  override def onInit() {
    state.neededContainers = state.containerCount
    containerManager = NMClient.createNMClient()
    containerManager.init(conf)
    containerManager.start

    logger.info(s"Requesting ${state.containerCount} containers")

    requestContainers(YarnConfig.getContainerMaxMemoryMb, YarnConfig.getContainerMaxCpuCores, state.neededContainers)
  }

  override def onShutdown {
    if (containerManager != null) {
      containerManager.stop
    }
  }

  override def onContainerAllocated(container: Container) {
    val containerIdStr = ConverterUtils.toString(container.getId)

    logger.info(s"Got a container from YARN ResourceManager: ${container}")
    val containerId = state.requestContainers.head  // TODO
    val task = HDFSUtils.moveHDFSToLocal(fs, state.tasks(containerId))  
    logger.info(s"""
      Launching a job on a new container.
      |containerId= ${container.getId()}
      |containerNode= ${container.getNodeId().getHost()}: ${container.getNodeId().getPort()}
      |containerNodeURI= ${container.getNodeHttpAddress()}
      |containerResourceMemory = ${container.getResource().getMemory()}
      |containerResourceVirtualCores ${container.getResource().getVirtualCores()}
        """.stripMargin);
    val launchContainer =
      new JobExecutor(container, containerManager, task);
    val launchThread = new Thread(launchContainer);
    launchThread.start()
    state.neededContainers -= 1
    if (state.neededContainers == 0) {
      state.jobHealthy = true
    }
    state.runningContainers += containerId -> new YarnContainer(container)
    logger.info(s"Started container ID ${containerId}")
  }

  override def onContainerCompleted(containerStatus: ContainerStatus) {
    val containerIdStr = ConverterUtils.toString(containerStatus.getContainerId)
    val containerId = state.runningContainers.filter { case (_, container) => container.id.equals(containerStatus.getContainerId()) }.keys.headOption

    containerId match {
      case Some(containerId) => {
        state.runningContainers -= containerId
      }
      case _ => None
    }

    containerStatus.getExitStatus match {
      case 0 => {
        logger.info(s"Container ${containerIdStr} completed successfully.")

        state.completedContainers += 1

        if (containerId.isDefined) {
          state.finishedContainers += containerId.get
          containerFailures -= containerId.get
        }

        if (state.completedContainers == state.containerCount) {
          logger.info("all containers have been completed successfully => SUCCEEDED")
          state.status = FinalApplicationStatus.SUCCEEDED
        }
      }
      case -100 => {
        logger.info("Got an exit code of -100.")

        state.releasedContainers += 1

        //  request a new container for the tasks due to node failure
        if (containerId.isDefined) {
          logger.info(s"Requesting a new container because of container failure ${containerIdStr}")

          state.neededContainers += 1
          state.jobHealthy = false

          // request a new container
          requestContainers(YarnConfig.getContainerMaxMemoryMb, YarnConfig.getContainerMaxCpuCores, 1)
        }
      }
      case _ => {
        logger.info(s"Container ${containerIdStr} failed with exit code ${containerStatus.getExitStatus}.")

        state.failedContainers += 1
        state.jobHealthy = false

        containerId match {
          case Some(containerId) =>
          
            state.neededContainers += 1
            val retryCount = YarnConfig.getContainerRetryCount
            val retryWindowMs = 100 //YarnConfig.getContainerRetryWindowMs

            if (retryCount == 0) {
              logger.error(s"Container ID ${containerId} failed => retry count is  0 => shutting down the application master")

              manyFailedContainers = true
            } else if (retryCount > 0) {
              val (currentFailCount, lastFailureTime) = containerFailures.get(containerId) match {
                case Some(ContainerFailure(count, lastFailure)) => (count + 1, lastFailure)
                case _ => (1, 0L)
              }
              if (currentFailCount > retryCount) {
                  containerFailures += containerId -> ContainerFailure(1, clock())
              } else {
                containerFailures += containerId -> ContainerFailure(currentFailCount, clock())
              }
            }

            if (!manyFailedContainers) {
              // Request a new container
              requestContainers(YarnConfig.getContainerMaxMemoryMb, YarnConfig.getContainerMaxCpuCores, 1)
            }
          case _ => None
        }
      }
    }
  }

  private def requestContainers(memMb: Int, cpuCores: Int, containers: Int) {
    logger.info(s"Requesting ${containers} container(s) with ${memMb} mb of memory")
    val capability = Records.newRecord(classOf[Resource])
    val priority = Records.newRecord(classOf[Priority])
    priority.setPriority(0)
    capability.setMemory(memMb)
    capability.setVirtualCores(cpuCores)
    (0 until containers).foreach(idx => amClient.addContainerRequest(new ContainerRequest(capability, null, null, priority)))
  }

}