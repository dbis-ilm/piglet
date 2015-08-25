package dbis.pig.plan

import java.io.File
import dbis.pig.mm.MaterializationPoint

import scala.io.Source
import java.io.PrintWriter
import java.io.FileWriter
import dbis.pig.tools.Conf
import java.nio.file.Path
import java.net.URI
import java.nio.file.Files
import com.typesafe.scalalogging.LazyLogging

class MaterializationManager(private val mapFile: Path, private val matBaseDir: URI) extends LazyLogging {

  private final val sep = ";"
  
  def this() = this(Conf.materializationMapFile, Conf.materializationBaseDir)
  
  
  logger.debug(s"base: $matBaseDir")
  logger.debug(s"mat map file: $mapFile")
  
  require(mapFile != null, "the mapFile must not be null")
//  require(mapFile.exists(), s"mapFile $mapFile does not exist")
  
  val f = mapFile.getParent
  logger.debug(s"checking mat map file parents: $f")
  if(!Files.exists(f))
    Files.createDirectories(f)
  
  if(!Files.exists(mapFile))
    Files.createFile(mapFile)
    
  require(Files.isReadable(mapFile) && Files.isWritable(mapFile), s"need to have read and write access to $mapFile" )
  require(matBaseDir != null, "Base directory for materialization must not be null")
  
  /**
   * Checks if we have materialized results for the given hash value
   * 
   * @param hash The hash value to get data for
   * @return Returns the path to the materialized result, iff present. Otherwise <code>null</code>  
   */
  def getDataFor(hash: String): Option[String] = {
    if(!Files.exists(mapFile))
      return None
    
    Source.fromFile(mapFile.toFile()).getLines().toStream
      .map { _.split(sep) }      // split file by ;
      .find { e => e(0) == hash }// get only the line starting with the required hash value
      .map { _(1) }              // get only the path value
  }  

  def generatePathForHash(hash: String): URI = {
    
    val matFile = matBaseDir.resolve(hash)
    
//    s"${matBaseDir.getCanonicalPath}${File.separator}$hash"
    matFile
  }
  
  def saveMapping(hash: String): URI = {
    saveMapping(hash, generatePathForHash(hash))
  }
  
  /**
   * Persist the given mapping of a hashcode to a specific file name.
   * 
   * @param hash The hash code of the sub plan to persist 
   * @param matFile The path to the file in which the results were materialized
   */
  def saveMapping(hash: String, matFile: URI): URI = {

    var writer: PrintWriter = null
    try {
      writer = new PrintWriter(new FileWriter(mapFile.toFile(), true))
      writer.println(s"$hash$sep$matFile")
    } finally {
      if(writer != null)
        writer.close()
    }
    
    matFile
  }
 }