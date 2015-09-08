package dbis.pfabric.deploy.mesos

import java.io.File
import org.apache.mesos._
import org.apache.mesos.Protos._
import java.io._
import scala.util.control.Breaks._
import java.util.concurrent.{ ExecutorService, Executors }
import com.typesafe.scalalogging.LazyLogging
import org.apache.mesos.Protos._
import org.apache.mesos.{ Executor, ExecutorDriver, MesosExecutorDriver }
import java.net.URL
import dbis.pfabric.deploy.util.MesosUtils

object MesosExecutor extends LazyLogging {

  def main(args: Array[String]) {
    System.loadLibrary("mesos")
    println("running task executor")
    var classLoader: ClassLoader = null
    var threadPool: ExecutorService = null

    val exec = new Executor {
      override def launchTask(driver: ExecutorDriver, task: TaskInfo): Unit = {
        val arg = task.getData.toByteArray
        val url = new URL(args(0))
        val fileName = url.getPath.split("/").last
        val path = MesosUtils.downloadFile(url, fileName)
        threadPool.execute(new Runnable() {
          override def run(): Unit = {
            println("execute now ...")
            val r = Runtime.getRuntime().exec(s"$path")
            val is = new BufferedReader(new InputStreamReader(r.getInputStream()))
            var line: String = ""
            breakable {
              while (true) {
                line = is.readLine()
                if (line == null)
                  break;
                println(line)
              }
            }

            println("In main after finsishing process")
            try {
              r.waitFor() // wait for process to complete
            } catch {
              case e: InterruptedException => println(e)
            }
            println("Process done, exit status was " + r.exitValue())
            try {
              driver.sendStatusUpdate(TaskStatus.newBuilder()
                .setTaskId(task.getTaskId)
                .setState(TaskState.TASK_FINISHED).build())
            } catch {
              case e: Exception => {
                logger.error("the execption is", e)
                logger.error("error in task id" + task.getTaskId.getValue)
                System.exit(1)
              }
            }
          }

        })
      }

      override def registered(driver: ExecutorDriver, executorInfo: ExecutorInfo, frameworkInfo: FrameworkInfo, slaveInfo: SlaveInfo): Unit = {
        threadPool = Executors.newCachedThreadPool()
      }
      override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = {}

      override def error(driver: ExecutorDriver, message: String): Unit = {}

      override def reregistered(driver: ExecutorDriver, slaveInfo: SlaveInfo): Unit = {}

      override def killTask(driver: ExecutorDriver, taskId: TaskID): Unit = {}

      override def disconnected(driver: ExecutorDriver): Unit = {}

      override def shutdown(driver: ExecutorDriver): Unit = {}
    }

    new MesosExecutorDriver(exec).run()
  }
}
