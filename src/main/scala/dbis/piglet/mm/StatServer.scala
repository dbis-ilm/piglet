package dbis.piglet.mm

import java.net.{InetSocketAddress, URLDecoder}
import scala.concurrent.duration._

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}


import dbis.piglet.tools.Conf
import dbis.piglet.tools.logging.PigletLogging

/**
  * The StatServer starts a HTTP Server on a specified port
  * and listens for incoming profiling information
  */
object StatServer extends PigletLogging {

  private lazy val port = Conf.statServerPort
  private lazy val allowedQueuedConnections = 100
  private lazy val system = ActorSystem("pigletstats")

  var server: HttpServer = _
  var writer: ActorRef = _

  def start(profilerSettings: ProfilerSettings): Unit = {
//    writer = system.actorOf(Props[StatsWriterActor], name = "statswriter")

    writer = system.actorOf(Props(new StatsWriterActor(profilerSettings)), name = "statswriter")

    server = HttpServer.create(new InetSocketAddress(java.net.InetAddress.getLocalHost.getHostAddress, port), allowedQueuedConnections)
    server.createContext("/times", new TimesHandler(writer))
    server.createContext("/sizes", new SizesHandler(writer))
    logger.info(s"Stats server will listen on $port")


    server.start()
    logger.debug("started stat server")
  }

  def stop(): Unit = {
    logger.debug("closing stat servers")
    if(server != null)
      server.stop(10.seconds.toSeconds.toInt) // 10 = timeout seconds

    // send the PoisonPill to the writer actor to stop it
    if(writer != null)
      writer ! PoisonPill

    // give the writer some time to process pending messages
    Thread.sleep(3.seconds.toSeconds)

    // shutdown the Akka system
    system.terminate()
  }

}

trait StatsHandler {

  def getData(qry: String): String = {
    val values = qry.split("=")
    require(values.length == 2)
    URLDecoder.decode(values(1), "UTF-8")
  }

}

class TimesHandler(private val writer: ActorRef) extends HttpHandler with StatsHandler with PigletLogging {

  override def handle(httpExchange: HttpExchange): Unit = {
    val msg = getData(httpExchange.getRequestURI.getQuery)

//    logger.debug(s"received time msg: $msg")

    httpExchange.sendResponseHeaders(200, -1)
    httpExchange.getRequestBody.close()

    writer ! TimeMsg(msg)
  }
}

class SizesHandler(private val writer: ActorRef) extends HttpHandler with StatsHandler {
  override def handle(httpExchange: HttpExchange): Unit = {
    val msg = getData(httpExchange.getRequestURI.getQuery)

    httpExchange.sendResponseHeaders(200, -1)
    httpExchange.getRequestBody.close()

    writer ! SizeMsg(msg)
  }
}

/**
  * An Akka Actor for asynchronously processing the messages that the HTTP received
  *
  */
class StatsWriterActor(profilerSettings: ProfilerSettings) extends Actor with PigletLogging  {

  /* Receive the message string, split it into its components and send it to the
   * DataflowProfiler
   */

  def receive = {
    case TimeMsg(msg) =>
//      logger.debug(s"received time msg: $msg")
      val arr = msg.split(StatsWriterActor.FIELD_DELIM)

      val lineage = arr(0)
      val partitionId = arr(1).toInt
      val parents = arr(2)
      val currTime = arr(3).toLong

      val parentsList = parents.split(StatsWriterActor.DEP_DELIM)
                          .filter(_.nonEmpty)
                          .map { s =>
                            s.split(StatsWriterActor.PARENT_DELIM)
                              .map(_.toInt)
                              .toList
                          }
                          .toList

      // store info in profiler

      DataflowProfiler.addExecTime(lineage, partitionId, parentsList, currTime)

    case msg: SizeMsg =>
      logger.debug(s"reiceived size msg: $msg")
      DataflowProfiler.addSizes(msg.values, profilerSettings.fraction)


    case msg =>
      logger.debug(s"received unknown msg: $msg")
  }
}

sealed trait StatMsg
case class TimeMsg(time: String) extends  StatMsg
case class SizeMsg(private val sizes: String) extends StatMsg {

  lazy val values = sizes.split(StatsWriterActor.FIELD_DELIM).map{s =>
    val a = s.split(StatsWriterActor.SIZE_DELIM)
    val lineage = a(0)
    val numRecords = a(1).toLong
    val numBytes = a(2).toLong / 2
    SizeInfo(lineage, records = numRecords, bytes = numBytes)
  }
}

//case class SizeMsg(value: String)

object StatsWriterActor {
  // keep in sync with [[dbis.piglet.backends.spark.PerfMonitor]]
  final val FIELD_DELIM = ";"
  final val PARENT_DELIM = ","
  final val DEP_DELIM = "#"
  final val SIZE_DELIM = ":"
}