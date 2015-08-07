package dbis.pig.tools

import java.io._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs._
import org.apache.hadoop.hdfs.DistributedFileSystem
import collection.JavaConversions._

/**
 * Created by kai on 06.08.15.
 */
object HDFSService {
  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("core-site.xml")
  private val hdfsHDFSSitePath = new Path("hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(conf)

  def isInitialized: Boolean = fileSystem.isInstanceOf[DistributedFileSystem]

  def copyToLocal(fromName: String, toName: String): Boolean = {
    val fromPath = new Path(fromName)
    val toPath = new Path(toName)
    fileSystem.copyToLocalFile(fromPath, toPath)
    true
  }

  def exists(fileName: String): Boolean = {
    val path = new Path(fileName)
    fileSystem.exists(path)
  }

  def copyToRemote(fromName: String, toName: String): Boolean = {
    val fromPath = new Path(fromName)
    val toPath = new Path(toName)
    fileSystem.copyFromLocalFile(fromPath, toPath)
    true
  }

  def listFiles(dir: String): Unit = {
    val path = new Path(dir)
    val iter = fileSystem.listFiles(path, false)
    while (iter.hasNext) {
      val f = iter.next
      println(f)
    }
  }

  def removeFile(filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.delete(path, true)
  }

  def createDirectory(dirPath: String): Unit = {
    val path = new Path(dirPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }

  def removeDirectory(dir: String, recursively: Boolean = false): Boolean = {
    val path = new Path(dir)
    fileSystem.delete(path, recursively)
  }
}