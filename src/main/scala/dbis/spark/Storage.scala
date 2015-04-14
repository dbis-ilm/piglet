package dbis.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd._

/**
 * Created by kai on 14.04.15.
 */
class PigStorage (val sc: SparkContext) {
  def load(path: String, delim: String = " "): RDD[Array[String]] = {
    sc.textFile(path).map(line => line.split(delim))
  }
}

object PigStorage {
  def apply(sc: SparkContext): PigStorage = {
    new PigStorage(sc)
  }
}

class RDFFileStorage(val sc: SparkContext) {
  def rdfize(line: String): Array[String] = {
    Array()
  }

  def load(path: String): RDD[Array[String]] = {
    sc.textFile(path).map(line => rdfize(line))
  }
}

object RDFFileStorage {
  def apply(sc: SparkContext): RDFFileStorage = {
    new RDFFileStorage(sc)
  }
}