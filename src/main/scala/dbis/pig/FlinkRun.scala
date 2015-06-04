package dbis.pig

import sys.process._

/**
  * Created by philipp on 04.06.15
  */

class FlinkRun extends Run{
  override def execute(master: String, className: String, jarFile: String){
    val flinkJar = sys.env.get("FLINK_JAR") match {
      case Some(n) => n
      case None => throw new Exception(s"Please set FLINK_JAR to your flink-dist jar file")
    }
    val run = s"java -Dscala.usejavacp=true -cp ${flinkJar}:${jarFile} ${className}"
    println(run)
    run !
  }
}
