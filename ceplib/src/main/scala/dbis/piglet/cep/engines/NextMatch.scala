package dbis.piglet.cep.engines

import dbis.piglet.backends.{SchemaClass => Event}
import scala.reflect.ClassTag
import dbis.piglet.cep.nfa.NFAStructure
import dbis.piglet.cep.nfa.NFAController
import dbis.piglet.cep.nfa.NormalState
import dbis.piglet.cep.ops.MatchCollector
class NextMatch[T <: Event: ClassTag](nfaController: NFAController[T], collector: MatchCollector[T]) extends CEPEngine(nfaController, collector) with Serializable {
   var statics: Long = 0
  override def runEngine(event: T): Unit = {
    runningStructursPool.foreach ( str => engineProcess(event, str))
    createNewStructue(event)
    runGCStructures()
  }
  private[NextMatch] def engineProcess(event: T, strInfo: (Long, NFAStructure[T])) {
    val currenStr=  strInfo._2
    val result: Int = checkPredicate(event, currenStr)
    if (result != -1) { // the predicate if ok.
      currenStr.addEvent(event, currenStr.getCurrentState.asInstanceOf[NormalState[T]].getEdgeByIndex(result))
      if (currenStr.complete) { //final state
        statics += 1
        collector + currenStr
        wantToDeletedStructurs += strInfo._1
      }
    }
  }
}