package dbis.events

import org.apache.spark.rdd.RDD

/**
 * Created by kai on 02.06.15.
 */
object Skyline {
  def process(input: RDD[List[Any]], numDims: Int, numPPD: Int, dominatesPredicate: String): RDD[List[Any]] = input
}
