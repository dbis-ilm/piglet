package dbis.pfabric.deploy.yarn.client

import scala.collection.JavaConversions._
import scala.collection.Map
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.util.Records
import dbis.pfabric.deploy.DeployException
import dbis.pfabric.deploy.TaskStatus._
import java.util.Collections
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.yarn.api.records.NodeState

/**
 * This class configures the client object which is responsible for
 * submitting the task to the resource manager (RM). It prepare the
 * ApplicationMaster (AM), and then submit the task. The user should provide
 * information such as the local resources (jars or files) that need to be
 * available for the task, the command that needs to be executed, and the
 * optional OS environment settings.
 *
 */
class YarnRMClient(conf: Configuration) extends LazyLogging {
  /**
   * the yarn client which is responsible for submitting the task to the
   * resource manager (RM).
   */

  val yarnClient = YarnClient.createYarnClient
  /**
   * the application master queue type
   */
  val amQueue: String = "default"
  logger.info(s"connecting to the resource manager ${conf.get(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS)}")
  /**
   * create and start a new client taking into consideration the current yarn configuration
   */
  yarnClient.init(conf)
  yarnClient.start
  var appId: Option[ApplicationId] = None

  /**
   * let the client to submit the application (task) to the resource manager
   * @param amMemory  the memory based resource requirements to be assigned for ApplicationMaster (AM) since it is considered as
   * normal container (0)
   * @param name the name of the task
   * @param amVCores the number of cores to be assigned for ApplicationMaster (AM)
   * @param cmd the command to be executed to run the ApplicationMaster (AM)
   * @param env the OS environmental variable
   */

  def submitApp(name: Option[String], amMemory: Int, amVCores: Int, localResources: Map[String, LocalResource], cmd: List[String], env: Map[String, String]): Option[ApplicationId] = {

    // reading some metrics from the cluster
    // using for debugging
    val clusterMetrics = yarnClient.getYarnClusterMetrics()
    logger.info(s"Got Cluster metric info from ASM, numNodeManagers= ${clusterMetrics.getNumNodeManagers()}")

    val clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING)
    logger.info("Got Cluster node info from ASM")
    for (node <- clusterNodeReports) {
      logger.info(s"""
        | Got node report from ASM for nodeId= ${node.getNodeId()}node.getNodeId() 
        | nodeAddress = ${node.getHttpAddress()}
        | nodeRackName = ${node.getRackName()}
        | nodeNumContainers = ${node.getNumContainers()}
        """.stripMargin)
    }

    val queueInfo = yarnClient.getQueueInfo(amQueue)
    logger.info(s"""
        | Queue info : queueName= ${queueInfo.getQueueName()}
        | queueCurrentCapacity= ${queueInfo.getCurrentCapacity()}
        | queueMaxCapacity= ${queueInfo.getMaximumCapacity()}
        | queueApplicationCount= ${queueInfo.getApplications().size()}
        | queueChildQueueCount= ${queueInfo.getChildQueues().size()}
        """.stripMargin)

    val listAclInfo = yarnClient.getQueueAclsInfo()
    for (aclInfo <- listAclInfo) {
      for (userAcl <- aclInfo.getUserAcls()) {
        logger.info(s"""
          | User ACL Info for Queue :  queueName= ${aclInfo.getQueueName()}
          | userAcl= ${userAcl.name()}
          """.stripMargin)
      }
    }
    val app = yarnClient.createApplication
    val appResponse = app.getNewApplicationResponse

    if (amMemory > appResponse.getMaximumResourceCapability().getMemory()) {
      throw DeployException(s"Max memory capabililty of resources in this yarn cluster is: ${appResponse.getMaximumResourceCapability.getMemory} and you are asking for more memory $amMemory than is allowed")
    }

    if (amVCores > appResponse.getMaximumResourceCapability().getVirtualCores()) {
      throw DeployException(s"Max core capabililty of resources in this yarn cluster is: ${appResponse.getMaximumResourceCapability.getVirtualCores} and you are asking for more cores $amVCores than is allowed")
    }

    appId = Some(appResponse.getApplicationId)

    logger.info(s"preparing app id: ${appId.get}")

    val appContext = app.getApplicationSubmissionContext
    val containerCtx = Records.newRecord(classOf[ContainerLaunchContext])
    val resource = Records.newRecord(classOf[Resource])
    val packageResource = Records.newRecord(classOf[LocalResource])

    containerCtx.setLocalResources(localResources)

    name match {
      case Some(name) => { appContext.setApplicationName(name) }
      case None       => { appContext.setApplicationName(appId.toString) }
    }

    containerCtx.setEnvironment(env)
    logger.info(s"set environment variables to ${env} for ${appId.get}")

    resource.setMemory(amMemory)
    logger.info(s"setting memory request to ${amMemory} for ${appId.get}")
    resource.setVirtualCores(amVCores)
    logger.info(s"setting cpu core request to ${amVCores} for ${appId.get}")
    appContext.setResource(resource)
    containerCtx.setCommands(cmd.toList)
    logger.info(s"setting command to ${cmd} for ${appId.get}")
    appContext.setApplicationId(appId.get)
    logger.info(s"set app ID to ${appId.get}")
    appContext.setAMContainerSpec(containerCtx)
    logger.info(s"submitting application request for ${appId.get}")
    yarnClient.submitApplication(appContext)
    appId
  }

  def status(appId: ApplicationId): Option[TaskStatus] = {
    val report = yarnClient.getApplicationReport(appId)
    logger.info(s"""Got application report from ASM for 
          | appId= ${appId.getId()} 
          | clientToAMToken= ${report.getClientToAMToken()} 
          | appDiagnostics= ${report.getDiagnostics()}
          | appMasterHost= ${report.getHost()}
          | appQueue= ${report.getQueue()}
          | appMasterRpcPort=  ${report.getRpcPort()}
          | appStartTime= ${report.getStartTime()}
          | yarnAppState= ${report.getYarnApplicationState().toString()}
          | distributedFinalState= ${report.getFinalApplicationStatus().toString()}
          | appTrackingUrl= ${report.getTrackingUrl()}
          """.stripMargin)

    convertState(report.getYarnApplicationState, report.getFinalApplicationStatus)
  }

  def kill(appId: ApplicationId) {
    yarnClient.killApplication(appId)
  }

  private def convertState(state: YarnApplicationState, status: FinalApplicationStatus): Option[TaskStatus] = {
    (state, status) match {
      case (YarnApplicationState.FINISHED, FinalApplicationStatus.SUCCEEDED) => Some(Successful)
      case (YarnApplicationState.KILLED, _) | (YarnApplicationState.FAILED, _) => Some(Unsuccessful)
      case (YarnApplicationState.NEW, _) | (YarnApplicationState.SUBMITTED, _) => Some(New)
      case _ => Some(Running)
    }
  }
}