package dbis.flink


import org.apache.flink.streaming.api.function.sink._
import org.apache.flink.util.Collector
import org.zeromq._

/**
  * Created by philipp on 09.06.15.
  */
class ZmqPublisher(addr: String) extends SinkFunction[List[String]]{ 

  var isRunning: Boolean = false

  override def invoke(in: List[String]) = {
    isRunning = true
    while (isRunning) {
      val context = ZMQ.context(1)
      val publisher = context.socket(ZMQ.PUB)
      publisher.bind(addr)
      while (true) {
        try {
          Thread.sleep (1000)
        } catch  {
          case e: InterruptedException => e.printStackTrace()
        }
        println(in.flatMap(_.getBytes))
        publisher.send(in.flatMap(_.getBytes).toArray, 0)
      }
    }
  }

  override def cancel() = {
    isRunning = false
  }

}
