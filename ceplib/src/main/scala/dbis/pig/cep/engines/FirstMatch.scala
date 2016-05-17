package dbis.pig.cep.engines

import dbis.pig.backends.{SchemaClass => Event}
import scala.reflect.ClassTag
import dbis.pig.cep.nfa.NFAStructure
import dbis.pig.cep.nfa.NFAController
import dbis.pig.cep.nfa.NormalState
import dbis.pig.cep.ops.MatchCollector
class FirstMatch[T <: Event: ClassTag](nfaController: NFAController[T], collector: MatchCollector[T]) extends CEPEngine(nfaController, collector) with Serializable {
   var statics: Long = 0
  
   override def runEngine(event: T): Unit = {
    if (runningStructursPool.size == 0)
      createNewStructue(event);
    else {
      engineProcess(event, runningStructursPool.head._2);
    }
  }
  private[FirstMatch] def engineProcess(event: T, currenStr: NFAStructure[T]) {
    val result: Int = checkPredicate(event, currenStr)
    if (result != -1) { // the predicate if ok.
      currenStr.addEvent(event, currenStr.getCurrentState.asInstanceOf[NormalState[T]].getEdgeByIndex(result))
      if (currenStr.complete) { //final state
        statics += 1
        println("complete")
        collector + currenStr
        runningStructursPool.clear()
      }
    }
  }
}