import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import dbis.spark._


object json {

def tupleAToString(t: List[Any]): String = {
  implicit def anyToSeq(a: Any) = a.asInstanceOf[Seq[Any]]

  val sb = new StringBuilder
  sb.append(t(0).map(s => s.toString).mkString("(", ",", ")"))
.append(",")
.append(t(1))
  sb.toString
}
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("json_App")
        val sc = new SparkContext(conf)
		val A = JsonStorage().load(sc, "/Users/kai/IdeaProjects/PigParser/src/it/resources/file.json")
    	val A_storehelper = A.map(t => tupleAToString(t)).coalesce(1, true)

    PigStorage().write("/Users/kai/IdeaProjects/PigParser/json.out", A_storehelper)

        sc.stop()
    }
}