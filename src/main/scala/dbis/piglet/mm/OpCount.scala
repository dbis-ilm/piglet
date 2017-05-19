package dbis.piglet.mm

import dbis.piglet.Piglet.Lineage
import dbis.piglet.tools.logging.PigletLogging

import scala.collection.mutable

/**
  * A data structure to maintain profiling information on operator occurrences
  * @param totalRuns Number of total runs of Piglet (in profiling mode)
  * @param adj The adjacency matrix
  */
case class OpCount(protected[mm] var totalRuns: Int = 0,
                   protected[mm] var adj: mutable.Map[Lineage, mutable.Map[Lineage, Int]]) extends PigletLogging {


  def incrRuns(n: Int = 1) = totalRuns += n

  def add(parent: Lineage, child: Lineage) = {

    require(parent != child, "an operator cannot be child of itself (parent must be != child): $parent")

    val l = adj.getOrElse(parent, mutable.Map.empty[Lineage, Int].withDefaultValue(0))

    l(child) += 1

    adj(parent) = l
  }

  def size = adj.size
}

object OpCount {
  def empty = new OpCount(0, mutable.Map.empty)
}