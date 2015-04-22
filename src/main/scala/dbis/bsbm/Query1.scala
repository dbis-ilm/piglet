package dbis.bsbm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._

object Query1 {
  val pattern = "([^\"]\\S*|\".+?\")\\s*".r

  def rdfize(line: String): Array[String] = {
    val fields = pattern.findAllIn(line).map(_.trim)
    fields.toArray.slice(0, 3)
  }

  def loadRDF(sc: SparkContext, path: String): RDD[Array[String]] = {
    sc.textFile(path).map(line => rdfize(line))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BSBMQuery1")
    // conf.setMaster("local[4]")
    val sc = new SparkContext(conf)

    val raw = loadRDF(sc, "usecases/testdata.nt")

    val producer = raw.filter(t =>
      t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/producer>").keyBy(t => t(2))
    val country = raw.filter(t =>
      t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/country>" &&
        t(2) == "<http://downlode.org/rdf/iso-3166/countries#GB>").keyBy(t => t(0))
    val products = producer.join(country).map{case (k,v) => (k, v._1(0), v._2(2))}.keyBy(t => t._2)
    /* (product => producer, product, country) */

    val productTypes = raw.filter(t =>
     t(1) == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>").keyBy(t => t(0))

    val productsWithTypes = productTypes.join(products).map{case (k, v) => (k, v._1(2), v._2._1, v._2._3) }
      .filter(t => t._2 != "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product>")
      .keyBy(t => t._1)
    /* product => product, type, producer, country */

    val reviews = raw.filter(t =>
      t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>").keyBy(t => t(2))
    val productReviews = reviews.join(productsWithTypes).map{case (k, v) => (v._1(0), v._2._1, v._2._2, v._2._3, v._2._4)}.keyBy(t => t._1)
    /* review => review, product, type, producer, country */

    val reviewerCountry = raw.filter(t =>
      t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/country>" &&
        t(2) == "<http://downlode.org/rdf/iso-3166/countries#ES>").keyBy(t => t(0))
    val reviewers = raw.filter(t => t(1) == "<http://purl.org/stuff/rev#reviewer>").keyBy(t => t(2))
    val reviewersWithCountry = reviewers.join(reviewerCountry).map{case (k,v) => (v._1(0), v._1(2), v._2(2))}.keyBy(t => t._1)
    /* review => review, reviewer, country */

    val completeReviews = productReviews.join(reviewersWithCountry)
      .map{case (k, v) => (v._1._1, v._1._2, v._1._3, v._1._4, v._1._5, v._2._2, v._2._3)}.keyBy(t => t._3)

    val groups = completeReviews.countByKey().toList.sortBy(- _._2).take(10)

    println(groups.mkString("\n"))

    // productsWithTypes.coalesce(1, true).saveAsTextFile("bsbm.out")

    sc.stop()
  }
}
