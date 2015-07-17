package dbis.pig.plan

import java.io.File
import scala.io.Source
import java.io.PrintWriter
import java.io.FileWriter
import dbis.pig.tools.Conf

class MaterializationManager(private val mapFile: File, private val matBaseDir: File) {

  private final val sep = ";"
  
  def this() = this(Conf.materializationMapFile, Conf.materializationBaseDir)
  
  require(mapFile != null, "the mapFile must not be null")
//  require(mapFile.exists(), s"mapFile $mapFile does not exist")
  
  val f = mapFile.getParentFile
  if(!f.exists())
    f.mkdirs()
  
  if(!mapFile.exists())
	  mapFile.createNewFile()

    
  require(mapFile.canRead() && mapFile.canWrite(), s"need to have read and write access to $mapFile" )
  require(matBaseDir != null, "Base directory for materialization must not be null")
  
  /**
   * Checks if we have materialized results for the given hash value
   * 
   * @param hash The hash value to get data for
   * @return Returns the path to the materialized result, iff present. Otherwise <code>null</code>  
   */
  def getDataFor(hash: String): Option[String] = {
    if(!mapFile.exists())
      return None
    
    Source.fromFile(mapFile).getLines().toStream
      .map { _.split(sep) }      // split file by ;
      .find { e => e(0) == hash }// get only the line starting with the required hash value
      .map { _(1) }              // get only the path value
  }  

  def generatePathForHash(hash: String): String = {
    val matFile = s"${matBaseDir.getCanonicalPath}${File.separator}$hash"
    matFile
  }
  
  def saveMapping(hash: String): String = {
    saveMapping(hash, generatePathForHash(hash))
  }
  
  /**
   * Persist the given mapping of a hashcode to a specific file name.
   * 
   * @param hash The hash code of the sub plan to persist 
   * @param matFile The path to the file in which the results were materialized
   */
  def saveMapping(hash: String, matFile: String): String = {

    var writer: PrintWriter = null
    try {
      writer = new PrintWriter(new FileWriter(mapFile, true))
      writer.println(s"$hash$sep$matFile")
    } finally {
      if(writer != null)
        writer.close()
    }
    
    matFile
  }
  
}