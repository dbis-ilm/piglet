package dbis.pfabric.deploy.util

import java.io.File
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.eclipse.jetty.server.handler.{DefaultHandler, HandlerList, ResourceHandler}
import java.net.InetAddress


class HttpServer(resourceBase: File) {
  var server: Server = null
  var port: Int = -1

  def start() = {
    if (server != null) {
      throw new RuntimeException("server already running")
    }
    else {

      val threadPool = new QueuedThreadPool()
      threadPool.setDaemon(true)

      server = new Server(0)
      server.setThreadPool(threadPool)

      val resourceHandler = new ResourceHandler
      resourceHandler.setResourceBase(resourceBase.getAbsolutePath)

      val handlerList = new HandlerList
      handlerList.setHandlers(Array(resourceHandler, new DefaultHandler))

      server.setHandler(handlerList)
      server.start()
      port = server.getConnectors()(0).getLocalPort
    }

  }

  def stop() {
    if (server == null) {
      throw new RuntimeException("server already stopped")

    }
    else {
      server.stop()
      server = null
      port = -1
    }

  }

  def uri: String = {
    if (server == null) {
      throw new RuntimeException("server not started")
    }
    else {
      return "http://" + getLocalIpAddress + ":" + port
    }


  }

  private def getLocalIpAddress: String = {
    // Get local IP as an array of four bytes
    InetAddress.getLocalHost().getHostAddress
  }
}
