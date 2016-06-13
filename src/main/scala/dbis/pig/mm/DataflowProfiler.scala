package dbis.pig.mm

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.ListBuffer

import java.nio.file.Path
import scala.io.Source
import scalikejdbc._
import dbis.pig.tools.DepthFirstTopDownWalker
import dbis.pig.plan.DataflowPlan
import dbis.pig.tools.logging.PigletLogging

import dbis.setm.SETM.timing
import dbis.pig.tools.BreadthFirstTopDownWalker
import dbis.pig.op.PigOperator

/**
 * Created by kai on 24.08.15.
 */
class DataflowProfiler extends PigletLogging {
  
  private val cache = MutableMap.empty[String, MaterializationPoint]

  def getMaterializationPoint(hash: String): Option[MaterializationPoint] = cache.get(hash)

  def addMaterializationPoint(matPoint: MaterializationPoint): Unit = {
    val entry = cache.getOrElse(matPoint.hash, matPoint)
    if (entry.parentHash.nonEmpty && cache.contains(entry.parentHash.get)) {
      // TODO: calculate _cumulative_ benefit
      val parent = cache.get(entry.parentHash.get).get
      val benefit = parent.loadTime + entry.procTime - entry.loadTime
      entry.benefit = parent.benefit + benefit
    }
    entry.count += 1
    cache += (matPoint.hash -> entry)
  }

  override def toString = cache.mkString(",")

  /**
   * Go over the plan and and check for existing runtime information for each op (lineage).
   * If something exists, create a materialization point that can be used later. 
   */
  def addMaterializationPoints(plan: DataflowPlan) = timing("identify mat points") {
    
    val walker = new DepthFirstTopDownWalker 
    walker.walk(plan) { op =>

      logger.debug( s"""checking database for runtime information for operator "${op.lineageSignature}" """)
      // check for the current operator, if we have some runtime/stage information 
      val avg = DB readOnly { implicit session =>
        sql"select avg(progduration) as pd, avg(stageduration) as sd from exectimes where lineage = ${op.lineageSignature} group by lineage"
          .map { rs => (rs.long("pd"), rs.long("sd")) }.single().apply()
      }

      // if we have information, create a (potential) materialization point
      if (avg.isDefined) {
        val (progduration, stageduration) = avg.get

        logger.debug( s"""found runtime information: program: $progduration  stage: $stageduration""")

        /* XXX: calculate benefit
         * Here, we do not have the parent hash information.
         * But is it still needed? Since we have the program runtime duration 
         * from beginning until the end the stage, we don't need to calculate
         * the cumulative benefit?
         */
        val mp = MaterializationPoint(op.lineageSignature,
          None, // currently, we do not consider the parent
          progduration, // set the processing time
          0L, // TODO: calculate loading time at this point
          0L, // TODO: calculate saving time at this point
          0 // no count/size information so far
        )

        this.addMaterializationPoint(mp)

      } else {
        logger.debug(s" found no runtime information")
      }
    }
    
  }
  
  /**
   * For profiling the jobs that are run with Piglet, we count how often an
   * operator lineage is used within a script. This counter value is added
   * and stored in our database. 
   * <br><br>
   * We traverse the plan and for operator we find, we store its count value
   * in a map. Afterwards, this map's content is stored in our database
   * where the values are added to already existing ones.
   * 
   * @param schedule The schedule, all current plans, to count operators in
   */
  def createOpCounter(schedule: Seq[(DataflowPlan, Path)]) = timing("analyze plans") {

    logger.debug("start creating lineage counter map")

    // the walker to traverse a plan
    val walker = new BreadthFirstTopDownWalker

    // the map to store count values
    val lineageMap = MutableMap.empty[String, Int]

    // the visitor to add/update the operator count in the map 
    def visitor(op: PigOperator): Unit = {
      val lineage = op.lineageSignature

      val value = lineageMap.getOrElse(lineage, 0)
      lineageMap.update(lineage, value + 1) // increment counter 
    }

    // traverse all plans and visit each operator within a plan
    schedule.foreach { plan => walker.walk(plan._1)(visitor) }

    // prepare map content to be stored into database (using batch insert)
    val entrySet = lineageMap.map { case (k, v) => Seq('id -> k, 'cnt -> v) }.toSeq
    
    DB localTx { implicit session =>
      /* this insert statement requires Postgres 9.5 (or higher):
       * we try to insert the id and count value, if there is a conflict on the ID (lineage) column,
       * it means that there already exists a value for this lineage, in this case we update the
       * existing value by adding the current value
       */
      sql"insert into opcount(id,cnt) values({id},{cnt}) on conflict (id) do update set cnt = opcount.cnt+{cnt};"
        .batchByName(entrySet: _*)
        .apply()
    }
    
    
    
    logger.warn("updating opcount not implemented yet.")
  }
  
}
