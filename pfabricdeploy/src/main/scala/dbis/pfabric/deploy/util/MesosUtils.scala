package dbis.pfabric.deploy.util

import java.io._
import java.util.UUID
import java.net.URL

object MesosUtils {

  def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File =
    {
      var attempts = 0
      val maxAttempts = 10
      var dir: File = null
      while (dir == null) {
        attempts += 1
        if (attempts > maxAttempts) {
          throw new IOException("Failed to create a temp directory " +
            "after " + maxAttempts + " attempts!")
        }
        try {
          dir = new File(root, "mesos-" + UUID.randomUUID.toString)
          if (dir.exists() || !dir.mkdirs()) {
            dir = null
          }
        } catch { case e: IOException =>  }
      }
      return dir
    }

  def copyStream(src: InputStream, out: OutputStream, closeStream: Boolean = false) = {
    val arrayBuffer = new Array[Byte](8192)
    var n = 0
    while (n != -1) {
      n = src.read(arrayBuffer)
      if (n != -1) {
        out.write(arrayBuffer, 0, n)
      }
    }
    if (closeStream) {
      src.close()
      out.close()
    }

  }

  def downloadFile(url: URL, localPath: String): String = {
    val stream = url.openStream()
    val path = System.getProperty("user.home") + "/" + localPath
    val file = new File(path)
    file.setExecutable(true)
    val out = new FileOutputStream(file)
    copyStream(stream, out, true)
    path
  }
}