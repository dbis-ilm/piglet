package dbis.pfabric.deploy.util
import com.typesafe.scalalogging.LazyLogging

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import java.io._
import java.net.URLDecoder
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.yarn.util.{ Apps, ConverterUtils }
import scala.collection.JavaConverters._
import org.apache.hadoop.yarn.api.records.LocalResource
import org.apache.hadoop.yarn.api.records.LocalResourceType
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility

/**
 *
 * @brief This class defines all the required information needed to run the task such
 * as the local Resources (binaries, jars, files etc.), Environment settings
 * (CLASSPATH etc.), the Command to be executed.
 *
 */
private[deploy] object HDFSUtils extends LazyLogging {

 
  /**
   * move the local Resources (binaries, jars, files etc.) to the HDFS in order to let all nodes in the cluster
   * to see them
   * @param fs HDFS file system
   * @param name the name of resource
   * @param path the path of resource
   */
  def getEnv(fs: FileSystem, suffix: String, iden: String, path: String, env: Map[String, String]): Unit = {
    logger.info("Setting env resources")
    // //hdfs://localhost:54310/user/hduser
    val dest = fs.getHomeDirectory() + suffix + "/" + iden
    moveLocalToHDFS(fs, dest, path)
    env += (iden -> dest)
  }

  def getLocalResource(fs: FileSystem, suffix: String, iden: String, path: String, localResources: Map[String, LocalResource]): Unit = {
    logger.info("Setting up local resources")
    // //hdfs://localhost:54310/user/hduser
    val dest = fs.getHomeDirectory() + suffix + "/" + iden
    localResources += (iden -> moveLocalToHDFS(fs, dest, path))

  }
  def moveLocalToHDFS(fs: FileSystem, hdfsPath: String, localPath: String): LocalResource = {
    val dest = new Path(hdfsPath)
    fs.copyFromLocalFile(new Path(localPath), dest)
    val srcFileStatus: FileStatus = fs.getFileStatus(dest)
    LocalResource.newInstance(
      ConverterUtils.getYarnUrlFromURI(dest.toUri()),
      LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
      srcFileStatus.getLen(), srcFileStatus.getModificationTime())
  }
  
  def moveHDFSToLocal(fs: FileSystem, hdfsPath: String): String = {
    val src = new Path(hdfsPath) 
    val dest: String = System.getProperty("user.home") + src.getName()
    fs.copyToLocalFile(false, src, new Path(dest));
    dest 
    
  }
  def createSystemFolder(fs: FileSystem, path: String): Unit = {
    val dest = new Path(fs.getHomeDirectory() + path)
    val ostream = FileSystem.create(fs, dest, new FsPermission("0710"))
    ostream.writeUTF(null)

  }
  def deleteSystemFolder(fs: FileSystem, path: String): Unit = {
    val dest = new Path(fs.getHomeDirectory() + path)
    println(dest.toUri().toString())
    fs.delete(dest, true); // delete file, true for recursive 
  }
  def getFileList(fs: FileSystem, path: String): Array[String] = {
    fs.listStatus(new Path(path)).map(_.getPath.toUri().toString())
  }
  
  def getCanonicalPath(file: String) = if (file.startsWith("/")) "" else new java.io.File(".").getCanonicalPath + "/"
}