package dbis.piglet.cep.engines

import dbis.piglet.backends.{SchemaClass => Event}
import scala.reflect.ClassTag
import scala.collection.mutable.ListBuffer
import dbis.piglet.cep.nfa.NFAStructure
import dbis.piglet.cep.nfa.NFAController
import dbis.piglet.cep.nfa.NormalState
import scala.collection.mutable.Map
import dbis.piglet.cep.ops.MatchCollector

abstract class CEPEngine[T <: Event: ClassTag](nfaController: NFAController[T], collector: MatchCollector[T]) extends Serializable {
   val structureID = { var sid: Long = 0; () => { sid += 1; sid } }
   var runningStructursPool: Map[Long, NFAStructure[T]] = Map()
   var wantToDeletedStructurs: ListBuffer[Long] = new ListBuffer()
  def createNewStructue(event: T): Unit = {
    val start = nfaController.getStartState
    start.edges.foreach { e =>
      if (e.evaluate(event)) {
        val newStr = new NFAStructure[T](nfaController)
        newStr.addEvent(event, e)
        runningStructursPool += (structureID() -> newStr)
      }
    }
  }
  def runGCStructures(): Unit = {
    if(runningStructursPool.size > 0) {
      runningStructursPool --=  wantToDeletedStructurs
    //wantToDeletedStructurs.foreach { x =>  runningStructursPool -= x  }
      wantToDeletedStructurs.clear()
    }
  }

  def checkPredicate(event: T, currenStr: NFAStructure[T]): Int = {
    var result: Int = -1
    if (currenStr.getCurrentState.isInstanceOf[NormalState[T]]) {
      val currentState = currenStr.getCurrentState.asInstanceOf[NormalState[T]]
      currentState.edges.zipWithIndex.foreach {
        case (e, i) =>
          if (e.evaluate(event, currenStr)) {
            result = i
          }
      }
    }
    result
  }
  def runEngine(event: T): Unit
  //def printNumMatches(): Unit
}