package dbis.flink

import org.apache.flink.streaming.api.scala._

/**
 * Created by philipp on 04.06.15.
 */
class PigStorage extends java.io.Serializable {
  def load(env: StreamExecutionEnvironment, path: String, delim: Char = ' '): DataStream[List[String]] = {
    env.readTextFile(path).map(line => line.split(delim).toList)
  }
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

  def load(env: StreamExecutionEnvironment, path: String): DataStream[Array[String]] = {
    env.readTextFile(path).map(line => rdfize(line))
  }
}

object RDFFileStorage {
  def apply(): RDFFileStorage = {
    new RDFFileStorage
  }
}
