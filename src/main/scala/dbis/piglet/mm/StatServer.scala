package dbis.piglet.mm

import java.net.{InetSocketAddress, URLDecoder}

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
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

  def start(): Unit = {
    server = HttpServer.create(new InetSocketAddress(port), allowedQueuedConnections)
    server.createContext("/times", new TimesHandler(writer))
    server.createContext("/sizes", new SizesHandler(writer))
    logger.info(s"Stats server will listen on $port")

    writer = system.actorOf(Props[StatsWriterActor], name = "statswriter")

    server.start()
    logger.debug("started stat server")
  }

  def stop(): Unit = {
    logger.debug("closing stat servers")
    if(server != null)
      server.stop(10) // 10 = timeout seconds

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

class TimesHandler(private val writer: ActorRef) extends HttpHandler with StatsHandler{

  override def handle(httpExchange: HttpExchange): Unit = {
    val msg = getData(httpExchange.getRequestURI.getQuery)

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
class StatsWriterActor extends Actor with PigletLogging  {

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

//      logger.debug(s"$lineage -> $partitionId : $parents")

      val parentsList = parents.split(StatsWriterActor.DEP_DELIM)
                          .filter(_.nonEmpty)
                          .map { s =>
                            s.split(StatsWriterActor.PARENT_DELIM)
                              .map(_.toInt)
                              .toSeq
                          }
                          .toSeq

      // store info in profiler
      DataflowProfiler.addExecTime(lineage, partitionId, parentsList, currTime)

    case msg: SizeMsg =>
//      logger.debug(s"received size msg: ${msg.values}")
      DataflowProfiler.addSizes(msg.values)
  }
}

sealed trait StatMsg
case class TimeMsg(time: String) extends  StatMsg
case class SizeMsg(private val sizes: String) extends StatMsg{
  lazy val values = sizes.split(StatsWriterActor.FIELD_DELIM).map{s =>
    val a = s.split(":")
    a(0) -> Some(a(1).toLong)
  }.toMap
}

object StatsWriterActor {
  // keep in sync with [[dbis.piglet.backends.spark.PerfMonitor]]
  final val FIELD_DELIM = ";"
  final val PARENT_DELIM = ","
  final val DEP_DELIM = "#"
}