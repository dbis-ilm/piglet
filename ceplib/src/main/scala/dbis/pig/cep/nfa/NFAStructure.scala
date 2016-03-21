package dbis.pig.cep.nfa
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import dbis.pig.backends.{SchemaClass => Event}

/**
 * @brief this class implements the execution instance of the NFA. 
 * Each NFA structure holds a pointer to the current state of the NFA 
 * and a set of events which conformed to the predicates till the current state (partial match) 
 * At the end, this structure may or may not have a match
 */
class NFAStructure[T <: Event: ClassTag](nfaController: NFAController[T]) extends scala.Cloneable with Serializable   {
  /**
   * stores the events of this structure which contribute 
   * on the complex event
   */
   var events: ArrayBuffer[T] = new ArrayBuffer()
  /**
   * which state the structure is at. At the beginning, the structure should
   * be at the start state of the NFA
   */
   var currenState: State[T] = nfaController.getStartState
  /**
   * boolean indicating whether the structure is complete 
   * to make a match or not
   */
   var complete: Boolean = false
  /**
   * gets the current state
   * @return the current state
   */
  def getCurrentState = currenState
  /**
   * adds an event to this structure, and makes necessary updates such as 
   * jumping to the next state and check whether the match is detected or not
   * @param event the event to be added
   * @param the current edge
   */
  def addEvent(event: T, currentEdge: ForwardEdge[T]): Unit = {
    events += event
    currenState = currentEdge.destState
    if (currenState.isInstanceOf[FinalState[T]])
      complete = true
  }
  
  override def clone(): NFAStructure[T] = {
    val copyStr = new NFAStructure[T](this.nfaController)
    copyStr.complete = this.complete
    copyStr.currenState = this.currenState
    copyStr.events = this.events.clone()
    //copyStr.events = this.events
    copyStr
  }
}