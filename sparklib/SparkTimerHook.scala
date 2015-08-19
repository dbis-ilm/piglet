
import org.apache.spark.SparkContext

object TimerHook {
  
  var start = 0L
  
  def beforeHook(id: String) { 
    start = System.currentTimeMillis()
  } 

  def afterHook(id: String, rdd: Option[org.apache.spark.rdd.RDD[_]]) {
    val duration = System.currentTimeMillis() - start

//    var cnt = 0L
//    if(rdd.isDefined) {
//      val sc = rdd.get.sparkContext
//      val accum = sc.accumulator(0L, id)
//      rdd.get.foreach { x => accum += 1; println(x) }
//      
//      cnt = accum.value
//    }
    
    // which is better? count or accum?    
    val cnt = if(rdd.isDefined) rdd.get.count() else 0 
    
    println(s"$id,$duration,$cnt")
  }

  def setupHook(sc: org.apache.spark.SparkContext) { }

  def shutdownHook(sc: org.apache.spark.SparkContext) {}
  
}