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
import dbis.pig.tools.logging.PigletLogging
import scalikejdbc._

/**
 * Manage where materialized intermediate results are stored
 */
class MaterializationManager(private val matBaseDir: URI) extends PigletLogging {

  def this() = this(Conf.materializationBaseDir)
  
  logger.debug(s"base: $matBaseDir")

  require(matBaseDir != null, "Base directory for materialization must not be null")
  
  /**
   * Checks if we have materialized results for the given hash value
   * 
   * @param hash The hash value to get data for
   * @return Returns the path to the materialized result, iff present. Otherwise <code>null</code>  
   */
  def getDataFor(hash: String): Option[String] = DB localTx { implicit session => 
    sql"""SELECT fname FROM materializations WHERE lineage = ${hash}"""
      .map { rs => rs.string("fname") }
      .single()
      .apply()
    }
    
  /**
   * Generate a path for the given lineage/hash value
   * 
   * @param hash The identifier (lineage) of an operator
   * @return Returns the path where to store the result for this operator
   */
  private def generatePathForHash(hash: String): URI = matBaseDir.resolve(hash)
  
  /**
   * Saves a mapping of the hash/lineage of an operator to its materilization location.
   */
  def saveMapping(hash: String): URI = {
    val matFile = generatePathForHash(hash)
    saveMapping(hash, matFile)
    matFile
  }
  
  
  /**
   * Persist the given mapping of a hashcode to a specific file name.
   * 
   * @param hash The hash code of the sub plan to persist 
   * @param matFile The path to the file in which the results were materialized
   */
  private def saveMapping(hash: String, matFile: URI) = DB localTx { implicit session => 
    sql"""insert into materializations VALUES(${hash}, ${matFile.toString()})"""
      .update.apply()
    }
    
 }