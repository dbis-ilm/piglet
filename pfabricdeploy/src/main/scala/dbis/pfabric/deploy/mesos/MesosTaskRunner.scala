package dbis.pfabric.deploy.mesos

import dbis.pfabric.deploy.TaskRunnerFactory
import dbis.pfabric.deploy.TaskRunner
import dbis.pfabric.deploy.config.MesosConfig

import scala.collection.mutable.Map
import org.apache.mesos._
import org.apache.mesos.Protos._
import com.typesafe.scalalogging.LazyLogging
import java.net.URI
/**
 * A factory for the Mesos task
 */
object MesosTaskRunnerFactory extends TaskRunnerFactory {
  /**
   * get the generated task to the system
   */
  override def getTaskRunner( task: URI, jarFile: URI, numberTasks: Int) = {
      new MesosTaskRunner( task.getPath, jarFile.getPath, numberTasks)
  }
}
/**
 *
 * A class represents the actual task to be executed in the cluster. laster the client will submit this task to the Mesos
 * and monitor the task.
 *
 */
class MesosTaskRunner( task: String, jarFile: String, numberTasks: Int) extends TaskRunner with LazyLogging {

  var driver: MesosSchedulerDriver = null
  lazy val framework = FrameworkInfo.newBuilder
    .setName(MesosConfig.getTaskName())
    .setCheckpoint(MesosConfig.getCheckPointEnable)
    .setRole("*")
    .setUser("") // Mesos can do this for us
    .build
  /**
   * a client to submit the task to the Mesos
   */

  def submit: MesosTaskRunner = {
    val scheduler = new MesosScheduler(task, numberTasks)
    driver = new MesosSchedulerDriver(scheduler, framework, MesosConfig.getMaster)
    driver.run()
    this
  }

  import dbis.pfabric.deploy.TaskStatus._
  def waitForFinish(timeoutMs: Long): TaskStatus = {
    val startTimeMs = System.currentTimeMillis()

    while (System.currentTimeMillis() - startTimeMs < timeoutMs) {
      Option(getStatus) match {
        case Some(s) => if (Successful.equals(s) || Unsuccessful.equals(s)) return s
        case None    => null
      }

      Thread.sleep(1000)
    }

    Running // TODO
  }

  def waitForStatus(status: TaskStatus, timeoutMs: Long): TaskStatus = {
    Running
  }
  def getStatus: TaskStatus = {
    Running //TODO
  }

  def kill: MesosTaskRunner = {
    //val status = if (driver.run() == Status.DRIVER_STOPPED) 0 else 1
    // Ensure that the driver process terminates.
    driver.stop()
    this
  }
}