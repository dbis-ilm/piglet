package dbis.piglet.cep.spark

import org.apache.spark.SparkContext
import dbis.piglet.cep.ops.SelectionStrategy._
import dbis.piglet.cep.ops.OutputStrategy._
import org.apache.spark.rdd._
import scala.reflect.ClassTag
import dbis.piglet.cep.nfa.NFAController
import dbis.piglet.backends.{SchemaClass => Event}

class CustomRDDMatcher[T <: Event: ClassTag](rdd: RDD[T]) {

  def matchNFA(nfa: NFAController[T], sstr: SelectionStrategy = FirstMatch, out: OutputStrategy = Combined) = {
    // println("create a new RDD matcher")
    val newRDD = rdd.coalesce(1, true)
    new RDDMatcher(newRDD, nfa, sstr, out)
  }

}

object CustomRDDMatcher {

  implicit def addRDDMatcher[T <: Event: ClassTag](rdd: RDD[T]) = {
    // println("add a custom RDD function")
    new CustomRDDMatcher(rdd)
  }
}