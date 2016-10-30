
package dbis.piglet.cep.ops
import dbis.piglet.backends.{SchemaClass => Event}

package object OutputTypes {
type PossibleTypes = Event with Boolean
}
/**
 * @brief this enumerations represents the available selection strategies
 * where the matching should be done accordingly.
 */
object SelectionStrategy extends Enumeration {
  type SelectionStrategy = Value
  val NextMatches, AllMatches, ContiguityMatches, FirstMatch, RecentMatch = Value
}

/**
 * @brief this enumerations represents the available output strategies
 * where the result output would be generated accordingly,
 * because the output of this operator is a complex event or a combination of tuples
 * one by one means generate the tuples one after another, in this case the resulting tuples
 * have fixed schema, whereas combined strategy combines all tuples (complex event) in one big tuple
 * which has variable schema
 *
 */
object OutputStrategy extends Enumeration {
  type OutputStrategy = Value
  val OneByOne, Combined, TrueValues = Value
}

/**
 * @brief this enumerations represents the available model strategies 
 * for processing or detecting the complex event accordingly.
 * in this engine, a non-deterministic finite automaton (NFA) based approach is used.
 * Moreover, we used a tree based evaluation model for pattern queries.
 * Each approach has its advantages and drawbacks in terms of performance,
 * optimization and its expressiveness
 *
 */
object MatchingStrategy extends Enumeration {
  type OutputStrategy = Value
  val TreeBased, NFABased = Value
}

