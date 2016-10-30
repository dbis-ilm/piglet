package dbis.piglet.cep.ops

import dbis.piglet.cep.engines._
import scala.reflect.ClassTag
import dbis.piglet.cep.ops.SelectionStrategy._
import dbis.piglet.cep.nfa.NFAController
import dbis.piglet.backends.{SchemaClass => Event}

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