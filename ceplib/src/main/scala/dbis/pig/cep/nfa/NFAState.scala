package dbis.pig.cep.nfa
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import dbis.pig.backends.{SchemaClass => Event}

/**
 * 
 * Star: zero or more
 * Plus: one or more
 * Question: at most one
 * Restricted: a particular iteration number
 */
object KleeneSpecification extends Enumeration {
  type KleeneSpecification = Value
  val Star, Plus, Question, Restricted = Value
}

import KleeneSpecification._
/**
 * @brief a NFA state object represents an automaton state in our engine. This state can
 * can be either start, kleene (repetition), final, normal or negated states. Every state has its own
 * behavior to detect a part from a complex event. It is used to know how the engine should process or store the
 * incoming tuples. Each incoming tuple should pass some of these states during the detection
 * of the complex event.
 *
 * a case class constructor for the state to set the state ID and its name. The name is a unique identifier for
 * this state which is necessary to present the state in the output.
 * @param id The state ID to set
 * @param name the name of the state to set
 */
abstract class State[T <: Event: ClassTag](id: Int, name: Option[String]) extends Serializable  {
  def this(id: Int) = this(id, None)
  /**
   * putput member variable information.
   * @return as above
   */
  override def toString: String = {
    return s"The state has an id  = $id and name = $name"
  }
}

/**
 * @brief a class represents the final state object in which the CEP can detect the complex
 * event once it reaches this state type. After that, the system produces
 * the results immediately
 * A case class constructor for the final state to set the state id and its name.
 * @param id the state id to set by the system or manually and should be unique
 * @param name the name of the state. It should be unique
 */
case class FinalState[T <: Event: ClassTag](id: Int, name: Option[String])
  extends State(id, name) with Serializable

/**
 * @brief a class represents the normal state object in which the CEP can traverse through
 * the NFA. This state must have forward edges to jump to a next state. Hence, the engine can
 * go from state to another by traversing the edges to reach the final state.
 *
 * a case class constructor for the normal state to set the state id and its name
 * @param  id the state id to set
 * @param name the name of the state
 */

class NormalState[T <: Event: ClassTag](id: Int, name: Option[String])
    extends State(id, name) with Serializable {
  /**
   * this array stores the forward edges of this state. By these edges, the engines
   * can go from state to another state.
   */
  var edges: ArrayBuffer[ForwardEdge[T]] = new ArrayBuffer()
  /**
   * adds an out-going edge associated to this state given the edge object
   * @param edge the edge object to be added to this state
   */
  def addEdge(edge: ForwardEdge[T]): Unit = edges += edge
  /**
   * adds an out-going edge to this state by specifying an edge id and a predicate associated with this edge
   * @param id the id of that edge
   * @param predicate the predicate of this edge
   */
  def addEdge(predicate: (T) => Boolean, id: Int): Unit = edges += new ForwardEdge(predicate, id, None)
  /**
   * gets the number of edges for this state
   * @return As above
   */
  def getNumEdges = edges.size
  /**
   * fetches a particular edge object from the edge collections given its index
   * @param index the index of the edge
   * @return as above
   */
  def getEdgeByIndex(index: Int): ForwardEdge[T] = edges(index)
  /**
   * Fetch a particular edge object from the edge collections given its id
   * @param id the id of the edge
   * @return as above
   */
  def getEdgeByID(id: Int): ForwardEdge[T] = null
}
/**
 * @brief a class represents the kleene state object. This kind of state has a loop edge
 * to stay in the same state unless a condition is satisfied to jump to the next state.
 *
 * a case class constructor for the kleene state to set the state id, name , forward edges for this state and the type
 * of kleene state
 * @param id the state id to set
 * @param name the name of the state
 * @param spec the type of this kleene state
 */
case class KleeneState[T <: Event: ClassTag](id: Int, spec: KleeneSpecification, name: Option[String])
  extends NormalState(id, name) with Serializable
/**
 * @brief a class represents the negated state object in which the CEP can detect the complex
 * event once reaches this state type
 * a case class constructor for the negation state to set the state id and its name
 * @param  id the state id to set
 * @param name the name of the state
 */
case class NegationState[T <: Event: ClassTag](id: Int, name: Option[String])
  extends NormalState(id, name) with Serializable
/**
 * @brief a class represents the start state object in which the CEP can start detecting
 * the complex event by the matcher operator and using the NFA controller
 * a case class constructor for the start state to set the state id and its name
 * @param  id the state id to set
 * @param name the name of the state
 */
case class StartState[T <: Event: ClassTag](id: Int, name: Option[String])
  extends NormalState(id, name) with Serializable



