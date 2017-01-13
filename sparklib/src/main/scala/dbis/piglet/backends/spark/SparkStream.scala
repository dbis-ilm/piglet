package dbis.piglet.backends.spark

import org.apache.spark._
import org.apache.spark.streaming._

object SparkStream {
  lazy val conf = new SparkConf()
  lazy val cx = new SparkContext(conf)
  lazy val ssc = new StreamingContext(cx, Seconds(1))
  
  def setAppName(appName: String) = conf.setAppName(appName)
  def setMaster(master: String) = conf.setMaster(master)
}
