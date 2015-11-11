package dbis.pig.backends.spark

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import scala.io.Source
import java.io.{ FileReader, FileNotFoundException, IOException }

class FileStreamReader(file: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  def onStart() {
    // Start the thread that reads data from a file
    new Thread("FileStreamReader") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing to do here
  }

  /** Create a reader to read  data from the file till EOF */
  private def receive() {
    try {
      for (line <- Source.fromFile(file).getLines()) {
        store(line)
        //Thread sleep 1000 // for testing
      }
      //stop("The EOF has been reached ... stop Now! ....")
    } catch {
      case ex: FileNotFoundException => println(s"Could not find $file file.")
      case ex: IOException           => println(s"Had an IOException during reading $file file")
    }
  }
}