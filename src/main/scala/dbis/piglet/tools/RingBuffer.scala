package dbis.piglet.tools

import scala.reflect.ClassTag

trait RingLike[T] extends Seq[T] {
  def put(o: T)
}

class RingBuffer[T: ClassTag](capacity: Int) extends RingLike[T] {

  require(capacity > 0, s"capacity must be > 0 , but is $capacity")

  private val ring = Array.fill(capacity){Option.empty[T]}

  private var curr = 0

  override def put(o: T): Unit = {
    require(o != null)

    ring(curr) = Some(o)

    curr = (curr + 1) % capacity
  }

  override def length: Int = ring.count(_.isDefined)

  override def foreach[U](f: (T) => U): Unit = iterator.foreach(f)

  override def apply(idx: Int): T = {
    require(idx > 0, s"idx must be > 0, but is $idx")
    val a = ring.apply(idx % capacity)

    if(a.isDefined)
      a.get
    else
      throw new ArrayIndexOutOfBoundsException("no such index $idx")
  }

  override def iterator: Iterator[T] = ring.iterator.filter(_.isDefined).map(_.get)
}

object RingBuffer {
  def apply[T:ClassTag](elements: T*): RingBuffer[T] = {
    val b = new RingBuffer[T](elements.length)
    for(e <- elements)
      b.put(e)

    b
  }
}