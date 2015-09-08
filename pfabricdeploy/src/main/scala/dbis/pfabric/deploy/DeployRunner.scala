package dbis.pfabric.deploy
import scopt.OptionParser
import com.typesafe.scalalogging.LazyLogging
import dbis.pfabric.deploy.yarn.client.YarnTaskRunner
import dbis.pfabric.deploy.yarn.client.YarnDeployTaskFactory
import TaskStatus._
import dbis.pfabric.deploy.util.KafkaManager
import dbis.pfabric.deploy.mesos.MesosTaskRunner
import dbis.pfabric.deploy.mesos.MesosTaskRunnerFactory
import java.net.URI

object DeployMethods extends Enumeration {
  val yarnDeploy = Methods("yarn")
  val mesosDeploy = Methods("mesos")
  val processDeploy = Methods("local")
  def Methods(name: String): Value with Matching =
    new Val(nextId, name) with Matching

  def unapply(s: String): Option[Value] =
    values.find(s == _.toString)

  trait Matching {
    def unapply(s: String): Boolean =
      (s == toString)
  }
}
import DeployMethods._
object DeployRunner extends LazyLogging {

  case class DeployConfig(method: String = "mesos",
                          input: String = null,
                          jarFile: String = null,
                          tasksNumber: Int = 1)
  val kafka: KafkaManager = null
  def main(args: Array[String]) {
    
    val parser = new OptionParser[DeployConfig]("dbis.pfabric.deploy.DeployRunner") {
      head("Deployment tool for PipeFabric Stream Processing Engine", "0.1")
      opt[String]('m', "master") required () action { (x, c) => c.copy(method = x) } text ("local, yarn, or mesos://<master-node>:5050 are supported")
      opt[String]('i', "input") required () action { (x, c) => c.copy(input = x) } text ("The task to be deployed")
      opt[String]('j', "jar") required () action { (x, c) => c.copy(jarFile = x) } text ("The jar file for deployment in case of yarn and mesos")
      opt[Int]('n', "tasks") required () action { (x, c) => c.copy(tasksNumber = x) } text ("The number of tasks")
    }

    parser.parse(args, DeployConfig()) match {
      case Some(config) => {
        submitTask(config.method, new URI(config.input), new URI(config.jarFile), config.tasksNumber)
      }
      case None =>
        return
    }
    
  }
  def submitTask(method: String,
                          task: URI,
                          jarFile: URI ,
                          tasksNumber: Int) {
    var job: TaskRunner = null
    method match {
      case DeployMethods.yarnDeploy() => {
        job = YarnDeployTaskFactory.getTaskRunner(task, jarFile, tasksNumber).submit()
      }
      case DeployMethods.mesosDeploy() => {
        job = MesosTaskRunnerFactory.getTaskRunner(task, jarFile, tasksNumber).submit()
      }
      case DeployMethods.processDeploy() => {
        job = ProcessTaskRunnerFactory.getTaskRunner(task, jarFile, tasksNumber).submit()
      }
    }

    logger.info("waiting for job to start")

    // waiting until the job has started, then exit.
    Option(job.waitForStatus(Running, 10000)) match {
      case Some(appStatus) => {
        if (Running.equals(appStatus)) {
          logger.info("job started successfully - " + appStatus)
        } else {
          logger.warn(s"unable to start job successfully. job has status ${appStatus}")
        }
      }
      case _ => logger.warn("unable to start job successfully.")
    }

    logger.info("exiting")
  }
}
  
