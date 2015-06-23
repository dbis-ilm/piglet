package dbis.pig.plan

import java.io.File
import scala.io.Source
import java.io.PrintWriter
import java.io.FileWriter

class MaterializationManager(val mapFile: File, val matBaseDir: File) {

  require(mapFile != null, "the mapFile must not be null")
  require(!mapFile.exists(), s"mapFile $mapFile does not exist")
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
    
    Source.fromFile(mapFile).getLines().toStream.map { _.split(";") }.find { e => e(0) == hash }.map { _(1) }
  }  

  def saveMapping(hash: String): String = {
    
    val matFile = s"${matBaseDir.getCanonicalPath}/$hash"
    
    var writer: PrintWriter = null
    try {
      writer = new PrintWriter(new FileWriter(mapFile, true))
      writer.println(s"$hash;$matFile")
    } finally {
      if(writer != null)
        writer.close()
    }
    
    matFile
  }
  
}