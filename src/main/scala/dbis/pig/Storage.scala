package dbis.pig

import org.apache.spark.SparkContext
import org.apache.spark.rdd._
//import org.apache.flink.streaming.api.scala._

/**
  * Created by Philipp on 08.05.15.
  */
class PigStorage extends java.io.Serializable {
  def load(sc: SparkContext, path: String, delim: String = " "): RDD[List[String]] = {
    sc.textFile(path).map(line => line.split(delim).toList)
  }
  /*
   def load(env: StreamExecutionEnvironment, path: String, delim: String = " "): DataStream[List[String]] = {
     env.readTextFile(path).map(line => {line.split(delim).toList} )
   }
   */
}

object PigStorage {
  def apply(): PigStorage = {
    new PigStorage
  }
}

class RDFFileStorage extends java.io.Serializable {
  val pattern = "([^\"]\\S*|\".+?\")\\s*".r

  def rdfize(line: String): Array[String] = {
    val fields = pattern.findAllIn(line).map(_.trim)
    fields.toArray.slice(0, 3)
  }

  def load(sc: SparkContext, path: String): RDD[Array[String]] = {
    sc.textFile(path).map(line => rdfize(line))
  }
  /*
   def load(env: StreamExecutionEnvironment, path: String): DataStream[Array[String]] = {
     env.readTextFile(path).map(line => {rdfize(line)} )
   }
   */
}

object RDFFileStorage {
  def apply(): RDFFileStorage = {
    new RDFFileStorage
  }
}
