package dbis.flink

import scala.Numeric.Implicits._


/**
 * Created by philipp on 26.05.15.
 */
object PigFuncs {
  def average[T: Numeric](bag: Iterable[T]) : Double = sum(bag).toDouble / count(bag).toDouble

  def count(bag: Iterable[Any]): Long = bag.size

  def sum[T: Numeric](bag: Iterable[T]): T = bag.sum

  def min[T: Ordering](bag: Iterable[T]): T = bag.min

  def max[T: Ordering](bag: Iterable[T]): T = bag.max

  def tokenize(s: String, delim: String = """[, "]""") = s.split(delim)

  def toMap(pList: Any*): Map[String, Any] = {
    var m = Map[String, Any]()
    for (i <- 0 to pList.length-1 by 2) { m += (pList(i).toString -> pList(i+1)) }
    m
  }
}
