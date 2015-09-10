package dbis.pfabric.deploy.yarn.appmaster

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.ContainerId
import java.net.URL

/**
 *  This class specify  the current state of the application master.
 */
class AMState(val taskId: Int, val containerId: ContainerId, val nodeHost: String, val nodePort: Int, val nodeHttpPort: Int) extends AppMasterListener {
  var completedContainers = 0
  var neededContainers = 0
  var failedContainers = 0
  var releasedContainers = 0
  var containerCount = 0
  var runningContainers = Map[Int, YarnContainer]()
  var finishedContainers = Set[Int]()
  var requestContainers = Set[Int]()
  var status = FinalApplicationStatus.UNDEFINED
  var jobHealthy = true
  var appAttemptId = containerId.getApplicationAttemptId
  var tasks: Array[String] = null
}