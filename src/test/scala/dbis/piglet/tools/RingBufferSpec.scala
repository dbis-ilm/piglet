package dbis.piglet.tools

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by hage on 31.05.17.
  */
class RingBufferSpec extends FlatSpec with Matchers {

  "A RingBuffer" should "return the correct length for empty buffer" in {
    val b = new RingBuffer[Int](3)

    b.length shouldBe 0
  }

  it should "return the correct length for single element in buffer" in {
    val b = RingBuffer(5)

    b.length shouldBe 1
  }

  it should "return the correct length more elements than capacity" in {
    val b = new RingBuffer[Int](3)

    Array(1,2,3,4,5,6,7,8).foreach(b.put)

    b.length shouldBe 3
  }

  it should "accept fewer elements as capacity" in {
    val b = new RingBuffer[Int](3)

    b.put(2)
    b.put(1)

    b should contain theSameElementsAs List(1, 2)
  }

  it should "accept same number of elements as capacity" in {
    val b = new RingBuffer[Int](3)

    b.put(2)
    b.put(1)
    b.put(3)

    b should contain theSameElementsAs List(3, 1, 2)
  }

  it should "remove the first entry when inserting cap + 1st element" in {
    val b = RingBuffer(1,2,3)

    b.put(4)

    b should contain theSameElementsAs List(4,2,3)
  }

  it should "accept more elements than capacity" in {

    val b = new RingBuffer[Int](3)

    Array(1,2,3,4,5,6,7,8).foreach(b.put)

    b should contain theSameElementsAs List(6,7,8)
  }

  it should "create a buffer with apply method" in {
    val b = RingBuffer(1,2,3,4,5,6,7,8)
    b should contain theSameElementsAs List(1,2,3,4,5,6,7,8)
  }
}
