package dbis.pig.tools

import java.io._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs._
import org.apache.hadoop.hdfs.DistributedFileSystem
import collection.JavaConversions._
import dbis.pig.tools.logging.PigletLogging
import java.nio.file.Files

/**
 * Created by kai on 06.08.15.
 */
object HDFSService extends PigletLogging {
  private val conf = new Configuration()
  
  val coreSite = Conf.hdfsCoreSiteFile.toAbsolutePath()
  val hdfsSite = Conf.hdfsHdfsSiteFile.toAbsolutePath()
  
  if(!Files.exists(coreSite))
    logger.warn(s"HDFS core site file does not exist at: $coreSite")
    
  if(!Files.exists(hdfsSite))
    logger.warn(s"HDFS hdfs site file does not exist at: $hdfsSite")
    
  
  private val hdfsCoreSitePath = new Path(coreSite.toString())
  private val hdfsHDFSSitePath = new Path(hdfsSite.toString())

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(conf)

  def isInitialized: Boolean = fileSystem.isInstanceOf[DistributedFileSystem]

  def process(cmd: String, params: List[String]): Unit = {
    logger.debug(s"HDFSService: process '${cmd}' with parameters ${params.mkString(",")}")
    cmd match {
      case "copyToLocal" => copyToLocal(params(0), params(1))
      case "copyToRemote" => copyToRemote(params(0), params(1))
      case "rm" => if (params.head == "-r") removeDirectory(params(1), true) else removeFile(params.head)
      case "rmdir" => removeDirectory(params.head)
      case "mkdir" => createDirectory(params.head)
      case "ls" => listFiles(if (params.isEmpty) "." else params.head)
      case _ => throw new java.lang.IllegalArgumentException("unknown fs command '" + cmd + "'")
    }
  }

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