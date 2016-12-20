package dbis.piglet

import dbis.piglet.mm.{DataflowProfiler, MaterializationPoint}
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by kai on 13.07.15.
 */
class DataflowProfilerSpec extends FlatSpec with Matchers {
  "The DataflowProfiler" should "accept several materialization points and adjust their benefits" in {
    val cacheMgr = new DataflowProfiler()
    cacheMgr.addMaterializationPoint(MaterializationPoint("m1", None, 10, 100, 100))
    cacheMgr.addMaterializationPoint(MaterializationPoint("m2", Some("m1"), 20, 80, 50))
    cacheMgr.addMaterializationPoint(MaterializationPoint("m3", Some("m2"), 40, 60, 40))
    cacheMgr.addMaterializationPoint(MaterializationPoint("m2", Some("m1"), 20, 80, 50))
    cacheMgr.addMaterializationPoint(MaterializationPoint("m4", Some("m3"), 15, 70, 60))
    cacheMgr.addMaterializationPoint(MaterializationPoint("m2", Some("m1"), 20, 80, 50))

    val om1 = cacheMgr.getMaterializationPoint("m1")
    om1 should not be empty
    val m1 = om1.get
    m1.hash should be ("m1")
    m1.count should be (1)
    m1.benefit should be (0)

    val om2 = cacheMgr.getMaterializationPoint("m2")
    om2 should not be empty
    val m2 = om2.get
    m2.hash should be ("m2")
    m2.count should be (3)
    m2.benefit should be (40)

    val om3 = cacheMgr.getMaterializationPoint("m3")
    om3 should not be empty
    val m3 = om3.get
    m3.hash should be ("m3")
    m3.count should be (1)
    m3.benefit should be (100)

    val om4 = cacheMgr.getMaterializationPoint("m4")
    om4 should not be empty
    val m4 = om4.get
    m4.hash should be ("m4")
    m4.count should be (1)
    m4.benefit should be (105)
  }
}