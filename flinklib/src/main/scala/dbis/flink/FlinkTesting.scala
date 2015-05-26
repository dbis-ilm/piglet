package dbis.flink

import org.apache.flink.streaming.api.scala._

object FlinkTesting{

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source: DataStream[(Int, String)] = env.fromElements(
      (1, "Who's there?"),
      (2, "I think I hear them. Stand, ho! Who's there?"))

    source.print
    source.writeAsCsv("./flinkTest.csv")
    source.writeAsText("./flinkTest.txt")

    env.execute
  }

}
