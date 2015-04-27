package dbis.bsbm

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Created by kai on 26.04.15.
 */
object Query4 {
  val pattern = "([^\"]\\S*|\".+?\")\\s*".r

  def rdfize(line: String): Array[String] = {
    val fields = pattern.findAllIn(line).map(_.trim)
    fields.toArray.slice(0, 3)
  }

  def loadRDF(sc: SparkContext, path: String): RDD[Array[String]] = {
    sc.textFile(path).map(line => rdfize(line))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BSBMQuery4")
    // conf.setMaster("local[4]")
    val sc = new SparkContext(conf)

    /* raw = LOAD 'usecases/testdata.nt' USING RDFFileStorage(); */

    val raw = loadRDF(sc, "usecases/testdata.nt")

    val products = raw.filter(t => t(1) == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" &&
      t(2) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType5>").keyBy(t => t(0))
    val offers = raw.filter(t =>
      t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/product>" ||
        t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/price>").keyBy(t => t(0))
    val groupedOffers = offers.groupByKey().map{case(k, v) => (k, v.toSeq(0)(2), v.toSeq(1)(2))}.keyBy(t => t._2)
    /* product => offer, product, price */
    val productPrices = products.join(groupedOffers).map{case(k, v) => v._2._3.toDouble} // .collect.map(t => println(t))
    /* price */
    val sumTotal = productPrices.reduce(_ + _)
    val countTotal = productPrices.count()
    println("sum = " + sumTotal + ", count = " + countTotal)
    //  .collect.map(t => println(t))

    val features = raw.filter(t =>
      t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature>").keyBy(t => t(0))
    val productsWithFeatures = products.join(features).map{case (k, v) => (k, v._2(2))}.keyBy(t => t._1)
    val featurePrices = productsWithFeatures.join(groupedOffers).map{case (k, v) => (v._1._2, v._2._3.toDouble)}
    /* feature, price */

    /* actually, we should use combineByKey */
    val featureStats = featurePrices.groupByKey().map{case (k, v) => (k, v.sum, v.size)}
    /* feature, sumF, countF */

    val ratios = featureStats.map(t => (t._1, // feature
      t._2 * (countTotal - t._3) / (t._3 * (sumTotal - t._2)) // (?sumF*(?countTotal-?countF))/(?countF*(?sumTotal-?sumF))
      )).sortBy(t => t._2, false) //.collect.map(t => println(t))
    val topRatios = ratios.take(100)

    println(topRatios.mkString("\n"))

    sc.stop()
  }
}
