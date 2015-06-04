package dbis.pig

import org.apache.spark.deploy.SparkSubmit

/**
  * Created by philipp on 04.06.15
  */
class SparkRun extends Run{
  override def execute(master: String, className: String, jarFile: String){
    SparkSubmit.main(Array("--master", master, "--class", className, jarFile))
  }
}
