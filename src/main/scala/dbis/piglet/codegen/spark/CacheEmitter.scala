package dbis.piglet.codegen.spark

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext}
import dbis.piglet.op.Cache

/**
  * Created by hage on 11.07.17.
  */
class CacheEmitter extends CodeEmitter[Cache] {
  override def template: String = "val <out> = <in>.persist(org.apache.spark.storage.StorageLevel.<mode>)"

  override def code(ctx: CodeGenContext, node: Cache): String = {

    val mode = node.cacheMode.toString

    val map = Map("out" -> node.outPipeName, "in" -> node.inPipeName, "mode" -> mode)

    render(map)
  }
}

object CacheEmitter {
  lazy val instance = new CacheEmitter
}
