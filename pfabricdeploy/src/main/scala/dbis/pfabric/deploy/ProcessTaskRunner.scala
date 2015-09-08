package dbis.pfabric.deploy

import TaskStatus._
import com.typesafe.scalalogging.LazyLogging
import scala.collection.mutable.ListBuffer
import java.io._
import java.net.URI
import scala.util.control.Breaks._

/**
 * A factory for the process task
 */
object ProcessTaskRunnerFactory extends TaskRunnerFactory {
  /**
   * get the generated task to the system
   */
  override def getTaskRunner(task: URI, supportingFile: URI, numberOfTasks: Int) = {
    new ProcessTaskRunner(task.getPath, supportingFile.getPath, numberOfTasks)
  }
}

class ProcessTaskRunner(task: String, supportingFile: String, numberOfTasks: Int) extends TaskRunner with LazyLogging {
  var jobStatus: Option[TaskStatus] = None
  var processes = new ListBuffer[Process]
  def runJob(job: String, taskId: Int): Unit = {
    val procThread = new Thread {
      override def run {
        val r = Runtime.getRuntime().exec(s"$job -i $taskId")
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
      }

    }
    procThread.start
  }
  def submit: ProcessTaskRunner = {
    jobStatus = Some(New)
    for( taskNumber <- 0 to numberOfTasks-1)  runJob(task, taskNumber)
    jobStatus = Some(Running)
    this
  }

  def kill: ProcessTaskRunner = {
    processes.foreach { p => p.destroy }
    jobStatus = Some(Unsuccessful)
    this
  }

  def waitForFinish(timeoutMs: Long) = {

    val thread = new Thread {
      setDaemon(true)
      override def run {
        try {
          processes.foreach { p => p.waitFor }
        } catch {
          case e: InterruptedException => logger.info("Got an interrupt exception", e)
        }
      }
    }

    thread.start
    thread.join(timeoutMs)
    thread.interrupt
    jobStatus.getOrElse(null)
  }

  def waitForStatus(status: TaskStatus, timeoutMs: Long) = {
    val start = System.currentTimeMillis

    while (System.currentTimeMillis - start < timeoutMs && status != jobStatus) {
      Thread.sleep(500)
    }

    jobStatus.getOrElse(null)
  }

  def getStatus = jobStatus.getOrElse(null)
}