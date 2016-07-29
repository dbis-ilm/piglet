
package dbis.pig.backends.spark
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import scala.io.Source
import java.io.{ FileNotFoundException, IOException }
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.StreamingContext

class FileStreamReader(file: String, @transient val ssc: StreamingContext) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

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
      //stop("stopped ...") // stop receiver
      //ssc.stop()
      //SparkStream.ssc.stop(true, true) // stop streaming context gracefully
    } catch {
      case ex: FileNotFoundException => println(s"Could not find $file file.")
      case ex: IOException           => println(s"Had an IOException during reading $file file")
    } finally {
      stop("Stopped Receiver")
      ssc.stop(true, true)
      SparkStream.ssc.stop(true, true)
      //sys.exit()
      
      
    }
  }
}
class FileReader(ssc: StreamingContext) {
  def readFile(file: String) = ssc.receiverStream(new FileStreamReader(file, ssc))
}
object FileStreamReader {
  implicit def customFileStreamReader(ssc: StreamingContext) =
    new FileReader(ssc)
}

