import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import dbis.spark._


object script {


    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("script_App")
        val sc = new SparkContext(conf)
		val A = PigStorage().load(sc, "/etc/passwd", ':')
        val B = A.map(t => List(t(0)))
        B.collect.map(t => println(t.mkString(",")))

        sc.stop()
    }
}