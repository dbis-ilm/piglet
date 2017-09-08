package dbis.piglet.tools

import java.io._
import java.nio.file.{Files, Paths}

import dbis.piglet.tools.logging.PigletLogging
import org.apache.hadoop.conf._
import org.apache.hadoop.fs.{FileSystem, _}
import org.apache.hadoop.hdfs.DistributedFileSystem

/**
 * Created by kai on 06.08.15.
 */
object HDFSService extends PigletLogging {
  
  
  private val conf = new Configuration()
  
  // val coreSite = Conf.hdfsCoreSiteFile.toAbsolutePath()
  // val hdfsSite = Conf.hdfsHdfsSiteFile.toAbsolutePath()
  val coreSite = Paths.get(scala.util.Properties.envOrElse("HADOOP_CONF_DIR", "/etc/hadoop/conf") + "/core-site.xml")
  val hdfsSite = Paths.get(scala.util.Properties.envOrElse("HADOOP_CONF_DIR", "/etc/hadoop/conf") + "/hdfs-site.xml")

  if(!Files.exists(coreSite))
    logger.warn(s"HDFS core site file does not exist at: $coreSite")

  if(!Files.exists(hdfsSite))
    logger.warn(s"HDFS hdfs site file does not exist at: $hdfsSite")
    
  
  private val hdfsCoreSitePath = new Path(coreSite.toString)
  private val hdfsHDFSSitePath = new Path(hdfsSite.toString)

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(conf)

  def isInitialized: Boolean = fileSystem.isInstanceOf[DistributedFileSystem]

  
  
  def process(cmd: HdfsCommand.HdfsCommand, params: List[String]): Unit = {
    logger.debug(s"HDFSService: process '$cmd' with parameters ${params.mkString(",")}")
//    HdfsCommand.withName(cmd.toUpperCase())
    cmd match {
      case HdfsCommand.COPYTOLOCAL => copyToLocal(params.head, params(1))
      case HdfsCommand.COPYTOREMOTE => copyToRemote(params.head, params(1))
      case HdfsCommand.RM => if (params.head == "-r") removeDirectory(params(1), recursively = true) else removeFile(params.head)
      case HdfsCommand.RMDIR => removeDirectory(params.head)
      case HdfsCommand.MKDIR => createDirectory(params.head)
      case HdfsCommand.LS => listFiles(if (params.isEmpty) "." else params.head)
      case HdfsCommand.CAT => showFile(params.head)
      case HdfsCommand.GETMERGE => mergeToLocal(params.slice(0, params.length-1), params.last)
//      case _ => throw new java.lang.IllegalArgumentException("unknown fs command '" + cmd + "'")
    }
  }

  private def statusString(fs: FileStatus): String = {
    def millisToDate(m: Long): String = {
      val d = new java.util.Date(m)
      val df = new java.text.SimpleDateFormat("dd MMM HH:mm")
      df.format(d)
    }

    s"${if (fs.isDirectory) "d" else "-"}${fs.getPermission.toString} " +
      s"${"%2d".format(fs.getReplication)} ${fs.getOwner} ${fs.getGroup} ${"%10d".format(fs.getLen)} " +
      s"${millisToDate(fs.getModificationTime)} ${fs.getPath.getName}"
  }

  def lastModified(path: String): Long = fileSystem.getFileStatus(new Path(path)).getModificationTime

  def mergeToLocal(fileList: List[String], toName: String): Boolean = {
    def appendToFile(inPath: Path, out: BufferedWriter) = {
      val is = fileSystem.open(inPath)
      val in = scala.io.Source.fromInputStream(is)
      in.getLines.foreach(l => out.write(l + "\n"))
    }

    val toPath = new Path(toName)
    val file = new File(toPath.toString)
    val writer = new BufferedWriter(new FileWriter(file))

    fileList.foreach { f =>
      val path = new Path(f)
      if (fileSystem.isFile(path)) {
        appendToFile(path, writer)
      }
      else {
        val lst = fileSystem.listStatus(path)
        lst.foreach { fStatus =>
          if (fStatus.isFile)
            appendToFile(fStatus.getPath, writer)
        }
      }
    }
    writer.close()
    true
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
    val lst = fileSystem.listStatus(path)
    lst.foreach { status =>
      println(statusString(status))
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

  def showFile(filename: String): Unit = {
    val path = new Path(filename)
    if (fileSystem.isFile(path)) {
      val is = fileSystem.open(path)
      val in = scala.io.Source.fromInputStream(is)
      in.getLines.foreach(println(_))
    }
  }
}