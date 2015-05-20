package dbis.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd._

/**
 * Created by kai on 14.04.15.
 */
class PigStorage extends java.io.Serializable {
  def load(sc: SparkContext, path: String, delim: String = " "): RDD[List[String]] = {
    sc.textFile(path).map(line => line.split(delim).toList)
  }

  /*
  def load(sc: SparkContext, path: String, schema: Schema, delim: String = " "): RDD[List[Any]] = {
    val pattern = "[^,(){}]+".r
    val fields = schema.element.valueType.fields
    sc.textFile(path).map(line => {
      val strings = pattern.findAllIn(line).toList
      for (f <- fields) {
      }
    })
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
}

object RDFFileStorage {
  def apply(): RDFFileStorage = {
    new RDFFileStorage
  }
}