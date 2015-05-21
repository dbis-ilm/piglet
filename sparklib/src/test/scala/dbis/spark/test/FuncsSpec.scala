package dbis.test.spark

import dbis.spark.PigFuncs
import org.scalatest._

/**
 * Created by kai on 17.04.15.
 */
class FuncsSpec extends FlatSpec with Matchers {

  "The max function" should "return the maximum of a list" in {
    val l1 = List(1, 2, 3, 4, 5)
    PigFuncs.max(l1) should be (5)

    val l2 = List(5, 4, 2, 2, 1)
    PigFuncs.max(l2) should be (5)

    val l3 = List(1.0, 4.0, 5.5, 2.2, 3.1)
    PigFuncs.max(l3) should be (5.5)

    val l4 = List("AAA", "BBBB", "ZZZZ", "DDDD")
    PigFuncs.max(l4) should be ("ZZZZ")
  }

  "The min function" should "return the minimum of a list" in {
    val l1 = List(1, 2, 3, 4, 5)
    PigFuncs.min(l1) should be (1)

    val l2 = List(5, 4, 2, 2, 1)
    PigFuncs.min(l2) should be (1)

    val l3 = List(1.0, 4.0, 5.5, 2.2, 3.1)
    PigFuncs.min(l3) should be (1.0)

    val l4 = List("XXXX", "AAA", "BBBB", "ZZZZ", "DDDD")
    PigFuncs.min(l4) should be ("AAA")
  }

  "The average function" should "return the average of a list" in {
    val l1 = List(1, 2, 3, 4, 5)
    PigFuncs.average(l1) should be(3)

    val l2 = List(5, 4, 2, 2, 1)
    PigFuncs.average(l2) should be(2.8)
  }

  "The count function" should "return the number of elements" in {
    val l1 = List(1, 2, 3, 4, 5)
    PigFuncs.count(l1) should be (5)

    val l2 = List()
    PigFuncs.count(l2) should be (0)
  }

  "The sum function" should "return the sum of the elements of list" in {
    val l1 = List(1, 2, 3, 4, 5)
    PigFuncs.sum(l1) should be (15)

    val l3 = List(1.0, 4.0, 5.5, 2.2, 3.1)
    PigFuncs.sum(l3) should be (15.8 +- 1e-5)
  }

  "The tokenize function" should "split a string on ','" in {
    val s = "1,2,3,4,5,6"
    PigFuncs.tokenize(s) should be (List("1", "2", "3", "4", "5", "6"))
  }

  "The tokenize function" should "split a string on ' '" in {
    val s = "1 2 3 4 5 6"
    PigFuncs.tokenize(s) should be (List("1", "2", "3", "4", "5", "6"))
  }

  "The tokenize function" should "split a string on '|'" in {
    val s = "1&2&3&4&5&6"
    PigFuncs.tokenize(s, "&") should be (List("1", "2", "3", "4", "5", "6"))
  }

  "The toMap function" should "produce a map from a list" in {
    PigFuncs.toMap("a", 1, "b", 2, "c", 3, "d", 4) should be (
      Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4)
    )
  }
}