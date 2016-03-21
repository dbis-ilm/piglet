package dbis.pig.cep.ops

import dbis.pig.cep.engines._
import scala.reflect.ClassTag
import dbis.pig.cep.ops.SelectionStrategy._
import dbis.pig.cep.nfa.NFAController
import dbis.pig.backends.{SchemaClass => Event}

abstract class EngineConf[T <: Event: ClassTag](nfa: NFAController[T], sstr: SelectionStrategy) {
  val collector: MatchCollector[T] = new MatchCollector()
  var engine: CEPEngine[T] = sstr match {
      case SelectionStrategy.FirstMatch        => new FirstMatch(nfa, collector)
      case SelectionStrategy.AllMatches        => new AnyMatch(nfa, collector)
      case SelectionStrategy.NextMatches       => new NextMatch(nfa, collector)
      case SelectionStrategy.ContiguityMatches => new ContiguityMatch(nfa, collector)
      case _                                   => throw new Exception("The Strategy is not supported")

  }
}
/*
trait EngineConfig [T] extends EngineConf[T] {
  implicit def event: Event
}*/