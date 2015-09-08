package dbis.pfabric.deploy
import scopt.OptionParser
import java.net.URI
import com.typesafe.scalalogging.LazyLogging
import TaskStatus._
import dbis.pfabric.deploy.util.KafkaManager
import dbis.pfabric.deploy.mesos.MesosTaskRunner
import dbis.pfabric.deploy.mesos.MesosTaskRunnerFactory
import dbis.pfabric.deploy.yarn.client.YarnTaskRunner
import dbis.pfabric.deploy.yarn.client.YarnDeployTaskFactory


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

  case class DeployConfig(master: String = "mesos",
                          task: String = null,
                          zookeeper: String = null,
                          jarFile: String = null,
                          tasksNumber: Int = 1)
  var kafka: KafkaManager = null
  def main(args: Array[String]) {
    
    val parser = new OptionParser[DeployConfig]("dbis.pfabric.deploy.DeployRunner") {
      head("Deployment tool for PipeFabric Stream Processing Engine", "0.1")
      opt[String]('m', "master") required () action { (x, c) => c.copy(master = x) } text ("local, yarn, or mesos://<master-node>:5050 are supported")
      opt[String]('i', "input") required () action { (x, c) => c.copy(task = x) } text ("The task to be deployed")
      opt[String]('j', "jar") required () action { (x, c) => c.copy(jarFile = x) } text ("The jar file for deployment in case of yarn and mesos")
      opt[Int]('n', "tasks") required () action { (x, c) => c.copy(tasksNumber = x) } text ("The number of tasks")
      opt[String]('z', "zookeepr") required () action { (x, c) => c.copy(zookeeper = x) } text ("The address of Zookeeper to prepare Kafka for handling tasks")
    }

    parser.parse(args, DeployConfig()) match {
      case Some(config) => {
        submitTask(config.master, new URI(config.task), new URI(config.jarFile), new URI(config.zookeeper),config.tasksNumber)
      }
      case None =>
        return
    }
    
  }
  def submitTask(master: String,
                          task: URI,
                          jarFile: URI,
                          zookeeper: URI,
                          tasksNumber: Int) {
    logger.info(s"starting  task submission with master equal to $master")
    logger.info(s"The task to be deployed exists in $task")
    logger.info(s"The number of tasks is set to $tasksNumber")
    logger.info(s" The zookeeper client address used to control Kafka server is $zookeeper")
    val method = master.split(":", 2)
    kafka = new KafkaManager(zookeeper.toString())
    logger.info("list Kafka topics which have been already registered") 
    logger.info(s"${kafka.describeAllTopics()}")
    logger.info("delete all registered Kafka topics") 
    kafka.deleteAllTopics()
    var job: TaskRunner = null
    logger.info(s"The method to be use for deployment is = ${method.head}") 
    method.head match {
      case DeployMethods.yarnDeploy() => {
        job = YarnDeployTaskFactory.getTaskRunner(task, jarFile, tasksNumber).submit()
      }
      case DeployMethods.mesosDeploy() => {
        if(!method(1).isEmpty()) {
          val uri = new URI(method(1).replaceAll("/", ""))
          logger.info(s"The mesos master address for deployment is $uri") 
          job = MesosTaskRunnerFactory.getTaskRunner(task, uri, tasksNumber).submit()
        }
        else 
          throw new IllegalArgumentException("Mesos deployment needs to specify the master URI such as mesos://host:5050")
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
    
    kafka.close()
  }
}
  
