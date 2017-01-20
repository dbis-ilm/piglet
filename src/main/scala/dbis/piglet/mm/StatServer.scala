package dbis.piglet.mm

import akka.actor.ActorSystem

import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.actor.Actor
import akka.actor.Props

import dbis.piglet.tools.logging.PigletLogging
import scala.concurrent.Future

object StatServer extends PigletLogging {
  
	implicit private val system = ActorSystem("piglethttp")
	implicit private val materializer = ActorMaterializer()
	// needed for the future flatMap/onComplete in the end
	implicit private val executionContext = system.dispatcher

	private var bindingFuture: Future[Http.ServerBinding] = null
	
	
	def start(port: Int) {
    
		val writer = system.actorOf(Props[StatsWriterActor], name = "statswriter")
		
    val route =
      path("times") {
        get {
          parameters('data.as[String]) { data =>
            writer ! data
            complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, "ok"))
          }
        }
      }

    bindingFuture = Http().bindAndHandle(route, "0.0.0.0", port)

    logger.info(s"Stats server online at port")
  }
   
  def stop(): Unit = {
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done

    system.terminate()  
  }
}

class StatsWriterActor extends Actor  {
  
  def receive = {
    case msg: String => 
      Console.err.println(msg)
  }
}