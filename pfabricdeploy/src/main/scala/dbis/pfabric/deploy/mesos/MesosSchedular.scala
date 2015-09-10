package dbis.pfabric.deploy.mesos

import org.apache.mesos.{ SchedulerDriver, Scheduler }
import org.apache.mesos.Protos._
import scala.collection.JavaConverters._
import scala.collection.mutable
import com.typesafe.scalalogging.LazyLogging
import dbis.pfabric.deploy.config.MesosConfig
import java.util.{ Collections, UUID }
import java.io.{ File, FileInputStream, FileOutputStream }
import dbis.pfabric.deploy.util._
import java.net.URI
class MesosScheduler(task: String, numberTasks: Int) extends Scheduler {
  var jarServer: HttpServer = null
  var finishedTasks: Int = 0
  var counter : Int = 400
  
  override def error(driver: SchedulerDriver, message: String) {}

  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {}

  override def slaveLost(driver: SchedulerDriver, slaveId: SlaveID) {}

  override def disconnected(driver: SchedulerDriver) {}

  override def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]) {}

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus) {
    println(s"received status update $status")

    System.out.println("Status update: task " + status.getTaskId().getValue() +
      " is in state " + status.getState().getValueDescriptor().getName())
    if (status.getState() == TaskState.TASK_FINISHED) {
      finishedTasks += 1
      println("Finished tasks: " + finishedTasks)
      if (finishedTasks == numberTasks) {
         println("Finished all tasks")
         driver.stop()
      }
    }

    if (status.getState() == TaskState.TASK_LOST ||
      status.getState() == TaskState.TASK_KILLED ||
      status.getState() == TaskState.TASK_FAILED) {
      println("Aborting because task " + status.getTaskId().getValue() +
        " is in unexpected state " +
        status.getState().getValueDescriptor().getName() +
        " with reason '" +
        status.getReason().getValueDescriptor().getName() + "'" +
        " from source '" +
        status.getSource().getValueDescriptor().getName() + "'" +
        " with message '" + status.getMessage() + "'")
      driver.abort()
    }

   // driver.acknowledgeStatusUpdate(status)
  }

  override def offerRescinded(driver: SchedulerDriver, offerId: OfferID) {}

  override def resourceOffers(driver: SchedulerDriver, offers: java.util.List[Offer]) {
    //for every available offer run tasks
    for (offer <- offers.asScala) {
      for( taskNumber <- 0 to numberTasks - 1){
        val cpus = Resource.newBuilder.
          setType(org.apache.mesos.Protos.Value.Type.SCALAR)
          .setName("cpus")
          .setScalar(org.apache.mesos.Protos.Value.Scalar.newBuilder.setValue(MesosConfig.getJobMaxCpuCores))
          .setRole("*")
          .build

        //generate random task id
        val taskId = "task" + System.currentTimeMillis()
        val taskInfo = TaskInfo.newBuilder()
          .setSlaveId(SlaveID.newBuilder().setValue(offer.getSlaveId.getValue).build())
          .setTaskId(TaskID.newBuilder().setValue(taskId.toString))
          .setExecutor(getExecutorInfo(task, taskNumber))
          .setName(UUID.randomUUID().toString)
          .addResources(cpus)
          .build()
        driver.launchTasks(Collections.singleton(offer.getId), Collections.singleton(taskInfo))

      }
    }
  }

  def getExecutorInfo(task: String, taskId: Int): ExecutorInfo = {
    counter = counter + 1 ;
    val scriptPath = MesosConfig.getExecutorLocation
    ExecutorInfo.newBuilder().
      setCommand(CommandInfo.newBuilder().setValue(
        s"/bin/sh ${scriptPath} $task -i $taskId"))
      .setExecutorId(ExecutorID.newBuilder().setValue(s"$counter"))
      .build()
  }

  override def reregistered(driver: SchedulerDriver, masterInfo: MasterInfo) {
      createFileServer(task)
  }

  override def registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo): Unit = {
  }

  def createFileServer(path: String): String = {
    val dirFile = MesosUtils.createTempDir()
    println("jar directory is" + dirFile.getAbsolutePath)
    val file = new File(path)
    val fileName = file.getName
    copyFile(file, new File(dirFile, fileName))

    jarServer = new HttpServer(dirFile)
    jarServer.start()
    val uri = jarServer.uri
    println("file server started at " + uri + "==>" + (uri + "/" + fileName))
    uri + "/" + fileName

  }

  private def copyFile(src: File, dest: File) = {
    val srcFile = new FileInputStream(src)
    val destFile = new FileOutputStream(dest)
    MesosUtils.copyStream(srcFile, destFile)
  }

}