package dbis.piglet.codegen.spark

import dbis.piglet.codegen.scala_lang.StoreEmitter

/**
  * Created by kai on 12.12.16.
  */

class StreamStoreEmitter extends StoreEmitter {
  override def template: String = """    <func>[<class>]().writeStream("<file>", <in><if (params)>, <params><endif>)""".stripMargin
}
