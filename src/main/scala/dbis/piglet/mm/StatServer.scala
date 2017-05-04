package dbis.piglet.mm

import akka.actor.{Actor, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import dbis.piglet.tools.logging.PigletLogging

import scala.concurrent.Future

/**
  * The StatServer starts a HTTP Server on a specified port
  * and listens for incoming profiling information
  */
object StatServer extends PigletLogging {
  
	implicit private val system = ActorSystem("piglethttp")
	implicit private val materializer = ActorMaterializer()
	// needed for the future flatMap/onComplete in the end
	implicit private val executionContext = system.dispatcher

	private var bindingFuture: Future[Http.ServerBinding] = null
	
	def start(port: Int) {

    // the Akka Actor to process incoming messages
	  val writer = system.actorOf(Props[StatsWriterActor], name = "statswriter")

    // define the route, i.e. http://<host>:<port>/times
    val route =
      path("times") {
        get {
          parameters('data.as[String]) { data => // parse HTTP parameter "data"
            writer ! data // send the String to the processing aktor
            complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, "ok")) // respond with OK
          }
        }
      }

    // start the server, listen on all addresses
    bindingFuture = Http().bindAndHandle(route, "0.0.0.0", port)

    logger.info(s"Stats server online at $port")
  }
   
  def stop(): Unit = {
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown Akka when done
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
    case msg: String =>
      val arr = msg.split(StatsWriterActor.FIELD_DELIM)

      val lineage = arr(0)
      val partitionId = arr(1).toInt
      val parents = arr(2)
      val currTime = arr(3).toLong

      logger.debug(s"$lineage -> $partitionId : $parents")

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
  }
}

object StatsWriterActor {
  // keep in sync with [[dbis.piglet.backends.spark.PerfMonitor]]
  final val FIELD_DELIM = ";"
  final val PARENT_DELIM = ","
  final val DEP_DELIM = "#"
}