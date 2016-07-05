package dbis.pig.cep.nfa

import scala.reflect.ClassTag
import scala.collection.mutable.ListBuffer
import dbis.pig.backends.{SchemaClass => Event}
import scala.collection.mutable.HashMap

/**
 * @brief a controller class to construct the NFA for detecting the complex event.
 * The user should create the states, edges and transitions by calling particular methods
 */
class NFAController[T <: Event: ClassTag] extends Serializable {
  
  type RelatedValueMap = HashMap[Int, ListBuffer[RelatedValue[T]]]
  /**
   * an edge counter to assign a unique id for each edge
   */
   val edgeID = { var eid = 0; () => { eid += 1; eid } }
  /**
   * a state counter to assign a unique id for each state
   */
   val stateID = { var sid = 0; () => { sid += 1; sid } }
  /**
   * a list to store the transitions between the states.
   * Each transition must contain an edge which has a predicate to evaluate
   */
   var transitions: ListBuffer[ForwardEdge[T]] = new ListBuffer()
  /**
   * a list to store all states except final, kleene and negated states and start state
   */
   var normalStates: ListBuffer[NormalState[T]] = new ListBuffer()
  /**
   * a list to store the final states. In any NFA,
   * we have multiple states from this type.
   */
   var finalStates: ListBuffer[FinalState[T]] = new ListBuffer()
  /**
   * the start state for this NFA
   */
   var startState: StartState[T] = null
   
   var initRelatedValue: Option[() => RelatedValueMap] = None
   
  /**
   * creates the start state for this NFA and assigns its name
    *
    * @param name the name of the start state
   */
  def createAndGetStartState(name: String): StartState[T] = {
    startState = new StartState(stateID(), Some(name))
    startState
  }
  /**
   * creates a normal state for this NFA and assign its name, this state should not
   * be final state or start state or kleene state
    *
    * @param name the name of this normal state
   * @return a pointer to the normal state
   */
  def createAndGetNormalState(name: String): NormalState[T] = {
    val normalState = new NormalState(stateID(), Some(name))
    normalStates += normalState
    normalState
  }
  /**
   * creates a final state for this NFA and assign its name
    *
    * @param name the name of the final state
   * @return a pointer to the final state
   */
  def createAndGetFinalState(name: String): FinalState[T] = {
    val finalState = new FinalState(stateID(), Some(name))
    finalStates += finalState
    finalState

  }
  /**
   * creates a forward edge for this NFA for a given predicate
    *
    * @param predicate the predicate of this edge
   * @return a pointer to a forward edge
   */
  def createAndGetForwardEdge(predicate: (T, HashMap[Int, ListBuffer[RelatedValue[T]]]) => Boolean): ForwardEdge[T] = {
    val transition = new ForwardEdge(predicate, edgeID(), None)
    transitions += transition
    transition
  }
  /**
   * creates a forward transition for this NFA between two states via an edge
    *
    * @param src the source state of this transition
   * @param dest the destination state of this transition
   * @param edge an edge to connect both the source and destination nodes
   */
  def createForwardTransition(src: State[T], edge: Edge[T], dest: State[T]): Unit = {
    val forwardEdge = edge.asInstanceOf[ForwardEdge[T]]
    val srcState = src.asInstanceOf[NormalState[T]]
    forwardEdge.setDestState(dest)
    srcState.addEdge(forwardEdge)
  }
  
  def setInitRelatedValue(init: () => RelatedValueMap) : Unit = initRelatedValue = Some(init)
  /**
   * get a pointer to the start state
    *
    * @return as above
   */
  def getStartState = startState
  /**
   * get the id of the start state
    *
    * @return as above
   */
  def getStartStateID(): Int = startState.id
} 