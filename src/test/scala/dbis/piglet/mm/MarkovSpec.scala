package dbis.piglet.mm

import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by hage on 02.06.17.
  */
class MarkovSpec extends FlatSpec with Matchers {

  "A Markov model" should "have size 0 when empty" in {
    val m = Markov.empty

    m.size shouldBe 0
  }

  it should "return the None cost if nothing was set" in {
    val m = Markov.empty

    m.add("start", Op("1"))

    m.cost("1") shouldBe empty

  }

  it should "return the correct cost of a node" in {
    val m = Markov.empty

    m.add("start", Op("1",T(3)))

    val t = T(3)

    m.cost("1") shouldBe defined
    m.cost("1").get shouldBe t
  }

  it should "find the correct parent infos" in {

    val m = Markov.empty

    m.add(Markov.startNode, "1")
    m.add("1", "2")
    m.add("2", "3")
    m.add("3", "4")
    m.add("2", "5")
    m.add("5", "4")

    m.parents("start") shouldBe empty

    m.parents("1") shouldBe defined
    m.parents("1").get should contain theSameElementsAs List("start")

    m.parents("4").get should contain theSameElementsAs List("3","5")

  }

  it should "have edge weights 1 for linear graph" in {
    val m = Markov.empty

    m.add(Markov.startNode, "1")
    m.add("1", "2")
    m.add("2", "3")
    m.add("3", "4")

    val pairs = List(("start","1"),("1","2"),("2","3"),("3","4"))

    pairs.foreach { e =>
      withClue(e.toString()) { m.rawWeight(e._1,e._2) shouldBe 1L }
    }
  }

  it should "have correct total cost for linear graph" in {
    val m = Markov.empty

    m.add(Markov.startNode, Op("1",T(3)))
    m.add("1", Op("2",T(2)))
    m.add("2", Op("3",T(4)))
    m.add("3", Op("4",T(3)))

    val c = m.totalCost("4",Markov.ProbMin)(Markov.CostMax)

    c shouldBe defined

    val cost = c.get._1
    val probs = c.get._2

    withClue("wrong cost") { cost shouldBe 12L }
    withClue("wrong prob") { probs shouldBe 1.0 }
  }

  it should "have correct total cost for graph with a split" in {
    val m = Markov.empty

    m.add(Markov.startNode, Op("1",T(3)))
    m.add("1", Op("2",T(2)))
    m.add("2", Op("3",T(4)))
    m.add("2", Op("8",T(2)))
    m.add("3", Op("4",T(3)))
    m.add("4", Op("5",T(1)))
    m.add("4", Op("6",T(2)))
    m.add("4", Op("7",T(3)))

    val c = m.totalCost("7",Markov.ProbMin)(Markov.CostMax)

    c shouldBe defined

    val cost = c.get._1
    val probs = c.get._2

    withClue("wrong cost") { cost shouldBe 15L }
    withClue("wrong prob") { probs shouldBe 1/3.0 }
  }

}
