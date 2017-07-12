package dbis.piglet.op

object CacheMode extends Enumeration {
  type CacheMode = Value
  val NONE,
      MEMORY_ONLY,
      MEMORY_AND_DISK,
      MEMORY_ONLY_SER,
      MEMORY_AND_DISK_SER,
      DISK_ONLY,
      MEMORY_ONLY_2,
      MEMORY_AND_DISK_2 = Value
}

import CacheMode.CacheMode

case class Cache (private[op] val out: Pipe,
                  private[op] val in: Pipe,
                  operatorId: String,
                  cacheMode: CacheMode) extends PigOperator(out, in, in.producer.schema ) {

  override def printOperator(tab: Int): Unit = {
    println(indent(tab)+s"CACHE { out = ${outPipeNames.mkString(",")} , in = ${inPipeNames.mkString(",")}")
    println(s"${indent(tab + 2)} schema = $schema , cache-mode: $cacheMode")
  }

  override def equals(other: Any) = other match {
    case o: Cache => operatorId == o.operatorId && outPipeName == o.outPipeName
    case _ => false
  }

  override def hashCode() = (operatorId+outPipeName).hashCode()


  override def toString = s"CACHE { out = ${outPipeNames.mkString(",")} , in = ${inPipeNames.mkString(",")}, schema = $schema , cache-mode: $cacheMode }"
}
