import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object BSBMQuery1 {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("BSBMQuery1")
        conf.setMaster("local[4]")
        val sc = new SparkContext(conf)
        sc.stop()
	}
}