package dbis.flink


import org.apache.flink.streaming.api.function.source._
import org.apache.flink.util.Collector
import org.zeromq._

/**
  * Created by philipp on 09.06.15.
  */
class ZmqSubscriber(addr: String) extends SourceFunction[Long]{ 

  var isRunning: Boolean = false

  override def run(ctx: Collector[Long]) = {
    isRunning = true
    while (isRunning) {
      val context = ZMQ.context(1)
      val subscriber = context.socket(ZMQ.SUB)
      subscriber.connect(addr)
    }
  }

  override def cancel() = {
    isRunning = false
  }

}
