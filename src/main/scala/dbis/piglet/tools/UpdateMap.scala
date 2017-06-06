package dbis.piglet.tools

import scala.collection.mutable.{Map => MutableMap}

class UpdateMap[K,V](m: MutableMap[K,V]) {

  def insertOrUpdate(k: K)( f: Option[V] => V): Unit = {

    if(m.contains(k)) {
      m(k) = f(Some(m(k)))
    } else {
      m(k) = f(None)
    }
  }
}

object UpdateMap {
  implicit def createUpdateMap[K,V](m: MutableMap[K,V]): UpdateMap[K,V] = new UpdateMap[K,V](m)
}
