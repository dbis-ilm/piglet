package dbis.piglet.mm

import dbis.piglet.tools.CliParams

object EvictionStrategy extends Enumeration {
  type EvictionStrategy = Value

  val NONE, LRU, KNAPSACK, KNAPSACK_RATIO = Value

  def getStrategy(v: EvictionStrategy): IEvictionStrategy = v match {
    case NONE => NoEviction
    case LRU => LRUEviction
    case KNAPSACK => KnapSackEviction
    case KNAPSACK_RATIO => KnapsackRatioEviction
  }
}

trait IEvictionStrategy {

  // needs to be a def otherwise test cases fail...
  def CACHE_SIZE = CliParams.values.profiling.get.cacheSize


  def wouldRemove(newEntry: CacheEntry, oldEntries: Seq[CacheEntry]): Seq[CacheEntry]

  /**
    * A helper method for finding those elements of a sorted list that will be removed
    * when adding a new entry to the cache
    *
    * @param initSize The initial size of the cache, e.g., the size of the new entry
    * @param toProcess The sorted list of cache entries to remove entries from
    * @return Returns a slice of the given cache list of elements that would be removed when
    *         adding a new entry
    */
  protected def toRemove(initSize: Long, toProcess: Seq[CacheEntry]): Seq[CacheEntry] = {
    var totalSize: Long = initSize

    val toRemoveIdx = toProcess.indexWhere{ e =>
      val fitsInCache = totalSize + e.bytes <= CACHE_SIZE
      if(fitsInCache)
        totalSize += e.bytes

      // indexWhere finds the first position where the condition is true, so we have
      // to negate this here, to find the first position where it does NOT fit into cache
      !fitsInCache
    }

    if(toRemoveIdx < 0)
      Seq.empty
    else
      toProcess.slice(toRemoveIdx, toProcess.size)
  }
}

object NoEviction extends IEvictionStrategy {
  override def wouldRemove(newEntry: CacheEntry, oldEntries: Seq[CacheEntry]): Seq[CacheEntry] = Seq.empty
}

object LRUEviction extends IEvictionStrategy {

  def wouldRemove(newEntry: CacheEntry, oldEntries: Seq[CacheEntry]): Seq[CacheEntry] = {

    if(newEntry.bytes > CACHE_SIZE)
      return Seq(newEntry)

    val toProcess = oldEntries.filter(e => !e.fixed && e.lastLoaded.isDefined).sortBy(_.lastLoaded.get)(Ordering[Long].reverse)

    val initCacheSize: Long = newEntry.bytes

    toRemove(initCacheSize, toProcess)

  }
}

object KnapSackEviction extends IEvictionStrategy {

  override def wouldRemove(newEntry: CacheEntry, oldEntries: Seq[CacheEntry]): Seq[CacheEntry] = {

    if(newEntry.bytes > CACHE_SIZE)
      return Seq(newEntry)


    val initCacheSize: Long = 0L
    val toProcess = oldEntries.+:(newEntry).filter(e => !e.fixed ).sortBy(_._benefit)(Ordering[Long].reverse)

    toRemove(initCacheSize, toProcess)
  }
}

object KnapsackRatioEviction extends IEvictionStrategy {
  override def wouldRemove(newEntry: CacheEntry, oldEntries: Seq[CacheEntry]): Seq[CacheEntry] = {
    if(newEntry.bytes > CACHE_SIZE)
      return Seq(newEntry)


    val initCacheSize: Long = 0L
    val toProcess = oldEntries.+:(newEntry).withFilter(e => !e.fixed )
          .map{ e => (e, e._benefit / e.bytes.toDouble)}
          .sortBy(_._2)(Ordering[Double].reverse)
          .map(_._1)

    toRemove(initCacheSize, toProcess)
  }
}
