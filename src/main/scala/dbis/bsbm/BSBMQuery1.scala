package dbis.bsbm

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object BSBMQuery1 {
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
        conf.setMaster("local[4]")
        val sc = new SparkContext(conf)
        
        val raw = loadRDF(sc, "bsbm.n3")
        
        val producer = raw.filter(t => t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/producer>").keyBy(t => t(0))
        val country = raw.filter(t => t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/country>" && t(2) == "<http://downlode.org/rdf/iso-3166/countries#GB>").keyBy(t => t(0))
        val products = producer.join(country)
        
        val productTypes = raw.filter(t => t(1) == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>").keyBy(t => t(0))
        val productsWithTypes = productTypes.join(products)
        
        val reviews = raw.filter(t => t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>").keyBy(t => t(2))
		val productReviews = reviews.join(productsWithTypes)
		
		val reviewerCountry = raw.filter(t => t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/country>" && t(2) == "<http://downlode.org/rdf/iso-3166/countries#KR>").keyBy(t => t(0)) 
		val reviewers = raw.filter(t => t(1) == "<http://purl.org/stuff/rev#reviewer>").keyBy(t => t(2))
		val reviewersWithCountry = reviewers.join(reviewerCountry)

		val completeReviews = productReviews.join(reviewersWithCountry)
		
		products.coalesce(1, true).map(println)
        sc.stop()
	}
}
