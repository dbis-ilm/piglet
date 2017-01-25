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
import java.nio.file.Path
import akka.stream.scaladsl.FileIO
import java.nio.file.StandardOpenOption
import java.nio.channels.AsynchronousFileChannel
import scalax.io.nio.ByteBuffer
import java.nio.ByteBuffer
import scala.collection.mutable.ListBuffer
import java.io.PrintWriter
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.BufferedOutputStream
import java.nio.file.Files

object StatServer extends PigletLogging {
  
	implicit private val system = ActorSystem("piglethttp")
	implicit private val materializer = ActorMaterializer()
	// needed for the future flatMap/onComplete in the end
	implicit private val executionContext = system.dispatcher

	private var bindingFuture: Future[Http.ServerBinding] = null
	
	var file: Option[Path] = None
	

	def start(port: Int, file: Path) {
    
	  StatServer.file = Some(file)

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

    logger.info(s"Stats server online at $port")
  }
   
  def stop(): Unit = {

    StatsWriterActor.writer.flush()
    StatsWriterActor.writer.close()
    
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}

object StatsWriterActor {
//  val writer = new PrintWriter(new BufferedWriter(new FileWriter(
//      StatServer.file.get.toFile(), // the file to write to 
//      true)))                       // true for APPEND mode
  
  val writer = new PrintWriter(Files.newBufferedWriter(StatServer.file.get, StandardOpenOption.APPEND, StandardOpenOption.CREATE))
}

class StatsWriterActor extends Actor  {
  
  def receive = {
    case msg: String =>
      StatsWriterActor.writer.println(msg)
//      StatsWriterActor.writer.flush()
      
  }
}

