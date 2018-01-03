package dbis.piglet

import java.io.File
import dbis.piglet.tools.HDFSService
import org.scalatest.{Matchers, FlatSpec}
import org.scalatest.Tag
import dbis.piglet.tools.HdfsCommand

object HdfsTest extends Tag("hdfs")

/**
 * Created by kai on 06.08.15.
 */
class HDFSSpec extends FlatSpec with Matchers {
  
  "The HDFS service" should "create a HDFS directoy" taggedAs HdfsTest in {
    if (HDFSService.isInitialized) {
      HDFSService.createDirectory("/data/blubs")
      // heck whether the directory exists
      HDFSService.exists("/data/blubs") should be(true)
    }
    else
      assume(false, "HDFS not enabled, no test performed")
  }

  it should "copy a file to HDFS" taggedAs HdfsTest in {
    if (HDFSService.isInitialized) {
      HDFSService.copyToRemote("LICENSE", "/data/blubs/LICENSE") should be(true)
      // check whether the file exists
      HDFSService.exists("/data/blubs/LICENSE") should be(true)

      HDFSService.copyToLocal("/data/blubs/LICENSE", "LICENSE-COPY")
      val localFile = new File("LICENSE-COPY")
      localFile.exists() should be(true)

      // cleanup
      HDFSService.removeFile("/data/blubs/LICENSE")
      localFile.delete()
    }
    else
      assume(false, "HDFS not enabled, no test performed")
  }

  it should "remove a directory from HDFS" taggedAs HdfsTest in {
    if (HDFSService.isInitialized) {
      HDFSService.removeDirectory("/data/blubs", true) should be(true)
      // check that the file doesn't exist anymore
      HDFSService.exists("/data/blubs") should be(false)
    }
    else
      assume(false, "HDFS not enabled, no test performed")
  }

  it should "process HDFS commands" taggedAs HdfsTest in {
    if (HDFSService.isInitialized) {
      HDFSService.process(HdfsCommand.MKDIR, List("/data/blubs"))
      HDFSService.exists("/data/blubs") should be(true)

      HDFSService.process(HdfsCommand.COPYTOREMOTE, List("LICENSE", "/data/blubs/LICENSE"))
      HDFSService.exists("/data/blubs/LICENSE") should be(true)

      HDFSService.process(HdfsCommand.COPYTOLOCAL, List("/data/blubs/LICENSE", "LICENSE-COPY"))
      val localFile = new File("LICENSE-COPY")
      localFile.exists() should be(true)
      HDFSService.process(HdfsCommand.RM, List("-r", "/data/blubs"))
      localFile.delete()
      HDFSService.exists("/data/blubs") should be(false)

    }
    else
      assume(false, "HDFS not enabled, no test performed")

  }
}
