package dbis.piglet.mm
import java.net.URI

import dbis.piglet.op.TimingOp
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.tools.DepthFirstTopDownWalker
import dbis.piglet.tools.logging.PigletLogging




/**
 * Manage where materialized intermediate results are stored
 */
class MaterializationManager(private val matBaseDir: URI) extends PigletLogging {
  
  logger.debug(s"materialization base directory: $matBaseDir")
//  logger.debug(s"using materialization storage service at $url")

  require(matBaseDir != null, "Base directory for materialization must not be null")


  def insertMaterializationPoints(plan: DataflowPlan, model: Markov): Unit = {

    DepthFirstTopDownWalker.walk(plan) {
      case _: TimingOp =>
      case op =>
        model.totalCost(op.lineageSignature, Markov.ProbMin)(Markov.CostMax) match {
          case Some((cost,prob)) =>
            val relProb = prob / model.totalRuns
            logger.debug(s"${op.name} (${op.lineageSignature})  : $cost ($relProb)")
          case None =>
        }


    }

  }




  /**
   * Checks if we have materialized results for the given hash value
   * 
   * @param hash The hash value to get data for
   * @return Returns the path to the materialized result, iff present. Otherwise <code>null</code>  
   */
  def getDataFor(hash: String): Option[String] = {
    
//    val result = scalaj.http.Http(url.resolve(s"${Conf.MATERIALIZATION_FRAGMENT}/${hash}").toString()).asString
//    
//    logger.debug(s"data for $hash: ${result}")
//    
//    if(result.isError) {
//      logger.warn(s"Could not retreive materialization info. ${result.statusLine}")
//      return None
//    }
//    
//    if(result.body.isEmpty())
//      return None
//    else {
//      import org.json4s._
//      import org.json4s.native.JsonMethods._
//      
//      val json = parse(result.body)
//      
//      val JString(lineage) = (json \ "lineage")
//      
//      if(lineage != hash) {
//        logger.error(s"Server sent wrong data. Requested Materialization info for $hash but got data for $lineage !")
//        return None
//      }
//      
//      val JString(path) = (json \ "path")
//      
//      return Some(path)
//    }
    
    ???
    
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
  private def saveMapping(hash: String, matFile: URI) = { 
    
//    val json = s"""{"lineage":"${hash}","path":"${matFile.toString()}"}"""
//    
//    val result = scalaj.http.Http(url.resolve(Conf.MATERIALIZATION_FRAGMENT).toString()).postData(json)
//        .header("Content-Type", "application/json")
//        .header("Charset", "UTF-8").asString
//
//    logger.debug(s"successfully sent data materialize data for $hash: ${result.body}")
    
    ???
        
  }
    
}
