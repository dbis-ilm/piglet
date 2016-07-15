
package dbis.pig.cep.nfa
import scala.reflect.ClassTag
import dbis.pig.backends.{SchemaClass => Event}
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

/**
 * @brief an object represents an automaton edge in our engine. In general, we have two types
 * of edges, forward edges to jump from state to state and loop edges to stay in the same state as a loop
 * Each edge has associated predicate to evaluate an incoming tuple against a condition. According to the result
 * of this condition, the engine will decide to jump to the next state or not.
 *
 * a predicate function to evaluate an incoming tuple, it returns boolean value to indicate
 * if the condition has been satisfied. If the condition is satisfied, the engine should store the incoming
 * tuple and jump to the next state. If not, the engine should stay in the current state unless receiving a tuple
 * that satisfied this condition.It takes two parameters. First, incoming tuples to evaluate
 * and second, some related values from other edges or previous stored tuples.
 *
 * a default constructor to set the edge id, its name (optional), and its predicate to evaluate the incoming tuple against
 * a condition.
 * @param id the edge id assigned to this edge
 * @param predicate the predicate assigned to this edge
 * @param name the edge name assigned to this edge (optional)
 */
abstract class Edge[T <: Event: ClassTag](predicate: (T, NFAStructure[T]) => Boolean, id: Int, name: Option[String]) extends Serializable  {
  /**
   * a constructor to set the predicate of the edge without assigning a name (None) and id (0)
   * @param predicate the predicate assigned to this edge
   */
  def this(predicate: (T, NFAStructure[T]) => Boolean) = this(predicate, 0, None)
  /**
   * a constructor to set the predicate of the edge without assigning a name (None)
   * @param predicate the predicate assigned to this edge
   * @param id the edge id assigned to this edge
   */
  def this(predicate: (T, NFAStructure[T]) => Boolean, id: Int) = this(predicate, id, None)
  /**
   * outputs member variable information.
   * @return as above
   */
  override def toString: String = {
    return s"The edge has id  = $id and name = $name with predicate = $predicate"
  }
  /**
   * evaluates incoming tuple by this edge. The edge sometimes needs some related
   * values from other edges. Therefore, the CEP structure has to be checked as well
   * @param in the incoming tuple
   */
  def evaluate(in: T, relatedValue: NFAStructure[T] = null): Boolean = predicate(in, relatedValue)
}

/**
 * @brief an object represents a forward edge in our engine, the CEP uses this kind
 * of edge to jump from state to state. Moreover, we should specify the destination state of this edge.
 * a default constructor to set the ID of the edge, its predicate, its name and its destination
 * @param id the edge ID assigned to this edge
 * @param predicate the predicate assigned to this edge
 * @param name the edge name assigned to this edge (optional)
 */
case class ForwardEdge[T <: Event: ClassTag](
  predicate: (T, NFAStructure[T]) => Boolean,
  id: Int,
  name: Option[String])
    extends Edge(predicate, id, name) with Serializable {
  /**
   * this variable stores the destination state of this forward edge.
   * it can be kleene, final and negated states
   */
  var destState: State[T] = null
  /**
   * set the destination state of this edge.
   * @param state the destination state to be set to this edge.
   */
  def setDestState(state: State[T]) = destState = state

}
/**
 * @brief  an object represents a loop edge in our engine. the system will stay in this
 * loop until some condition should be satisfied such as reaching the limit of
 * allowed loop
 * a default constructor to set the ID of the edge, its predicate, its name and its destination
 * @param id the edge ID assigned to this edge
 * @param numOfLoop specify a limit for the number of loop
 * @param predicate the predicate assigned to this edge
 * @param name the edge name assigned to this edge (optional)
 */
case class LoopEdge[T <: Event: ClassTag](
  predicate: (T, NFAStructure[T]) => Boolean,
  id: Int,
  numOfLoop: Int = Int.MaxValue,
  name: Option[String])
    extends Edge(predicate, id, name) with Serializable