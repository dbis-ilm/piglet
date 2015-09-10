package dbis.pfabric.deploy.yarn.appmaster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.NMClient
import com.typesafe.scalalogging.LazyLogging
import scala.collection.JavaConverters._
import java.util.Collections

/**
 * 
 * This class is responsible for lunching the task (or the container) on a
 * particular node in the cluster. It receive the executable files or the tasks
 * and execute them in the containers.
 * Assigned the required objects to lunch this task
 * @param  lcontainer the allocated container for this task
 * @param nmClientAsync MClientAsync object to communicate with NodeManagers.
 * @param the executable file to be executed by this container
 */
class JobExecutor (lcontainer: Container , nmClientAsync: NMClient ,
      execFile: String) extends LazyLogging with Runnable {

	/**
	 * execute the task in a particular container aster setting its command
	 */
	def run() {
		logger.info("Setting up container launch container for containerid="
				+ lcontainer.getId());

		// set the local resources
    val localResources = collection.mutable.Map[String,LocalResource]()
	
    val command = s"""
        | ${this.execFile} 
        | 1>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/${execFile}.${ApplicationConstants.STDOUT} 
        | 2>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/${execFile}.${ApplicationConstants.STDERR}
        """.stripMargin.replaceAll("\n", " ")

		logger.info("Command to be run  :  " + command);

		val ctx = ContainerLaunchContext.newInstance(
				localResources.asJava, null, List(command).asJava, null, null, null);
		//containerListener.addContainer(container.getId(), container);
		// start the task
		nmClientAsync.startContainer(lcontainer, ctx);
	}
}
