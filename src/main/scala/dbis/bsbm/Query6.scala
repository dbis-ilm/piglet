package dbis.bsbm

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import dbis.spark.PigFuncs

object Query6 extends App {

  val pattern = "([^\"]\\S*|\".+?\")\\s*".r

  def rdfize(line: String): Array[String] = {
    val fields = pattern.findAllIn(line).map(_.trim)
    fields.toArray.slice(0, 3)
  }

  def loadRDF(sc: SparkContext, path: String): RDD[Array[String]] = {
    sc.textFile(path).map(line => rdfize(line))
  }
  
  
  if(args.length != 4) {
      println("Usage: program <inputFile> <logFile> <load-flag> <save-flag>")
      System.exit(1)
  }

  val dataFile = args(0)
  val logFile = args(1)
  val load = args(2).toBoolean
  val save = args(3).toBoolean

  val theProducer = "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer1/Producer1>"
  
  val conf = new SparkConf().setAppName("BSBMQuery6")
  // conf.setMaster("local[4]")
  val sc = new SparkContext(conf)

  val t1 = System.currentTimeMillis()

  //val raw = loadRDF(sc, "hdfs:///data/iswc2015/testdata.nt")*/
  
  // raw = LOAD 'theFile'
  val raw = loadRDF(sc, dataFile)
  
  /* reviewedProducts = BGP_FILTER raw BY { ?product bsbm:producer %Producer% .
                                         ?review bsbm:reviewFor ?product . }
                                          
  */
  val products = raw.filter { t => 
    t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/producer>" && 
    t(2) == theProducer }.keyBy { t => t(0) }
     
  val reviews = raw.filter(t =>
    t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>").keyBy(t => t(2))
  
  val reviewedProducts = products.join(reviews)
                            .map{ case (k,v) => (k, v._2(0))} // (product, review)
                            .keyBy{ t => t._2}
  
  
                            
  // reviewRatingX = BGP_FILTER raw BY { ?review bsbm:ratingX ?score }
  // X: 1..4
                            
  val reviewRating1 = raw.filter { t => t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>" }
  val reviewRating2 = raw.filter { t => t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>" }
  val reviewRating3 = raw.filter { t => t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating3>" }
  val reviewRating4 = raw.filter { t => t(1) == "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating4>" }
  
  // reviewRating = UNION reviewRating1, reviewRating2, reviewRating3, reviewRating4   
  val reviewRating = (reviewRating1.union(reviewRating2).union(reviewRating3).union(reviewRating4)).keyBy { t => t(0) }
  
  // productRating = JOIN reviewedProducts BY $?, reviewRating BY $?
  val productRating = reviewedProducts.join(reviewRating).map{ case (k,v) => v._2(2) } // score
  
  // reviewer = BGP_FILTER raw BY { ?review rev:reviewer ?reviewer . }
  val reviewer = raw.filter { t => t(1) == "<http://purl.org/stuff/rev#reviewer>" }.keyBy { t => t(0) } 
  
  
  
  // reviewedProductsWithReviewer = JOIN reviewedProducts BY $?, reviewer BY $?
  val reviewedProductsWithReviewer = reviewedProducts.join(reviewer)
                                        .map{ case (k,v) => (k, v._1._1, v._2(2))} // (review, product, reviewer)
                                        .keyBy{t => t._1}
  
  // withRatings = JOIN reviewedProductsWithReviewer BY $?, reviewRating BY $?                                        
  val withRatings = reviewedProductsWithReviewer.join(reviewRating)
                      .map{ case (k,v) => (v._1._3, v._2(2)) } // reviewer, score
                      .keyBy{ t => t._1 }
  
  // grouped = FOREACH (GROUP withRatings BY $?) GENERATE (group, AVG($?) AS avg)                       
  val grouped = withRatings.groupByKey().map{ case (k,v) => 
      val scores = v.map{ case (r,s) => s.toDouble }
      val avgScore2 = PigFuncs.average(scores)
      
      (k, avgScore2)
  
  } 
    
  // avgScore = FOREACH (GROUP productsRating ALL) GENERATE AVG(productsRating.$?)
  // cross = CROSS grouped, avgScore 
  // having = FILTER cross BY $1 > ($2 * 1.5)
  // 
  val avgScore = productRating.map { v => v.toDouble }.sum() / productRating.count()
  val having = grouped.filter{ t => t._2 > avgScore *1.5 }
  
  val res = having.collect().foreach { x => println(x.toString())}
  
        
 
}