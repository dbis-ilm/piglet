package dbis.pig.cep.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}
import scala.reflect.ClassTag
import dbis.pig.cep.nfa.NFAController
import dbis.pig.cep.engines._
import dbis.pig.cep.ops.SelectionStrategy._
import dbis.pig.cep.ops.OutputStrategy._
import dbis.pig.backends.{SchemaClass => Event}
import dbis.pig.cep.ops.MatchCollector
import dbis.pig.cep.ops.SelectionStrategy

class RDDMatcher[T <: Event: ClassTag](parent: RDD[T], nfa: NFAController[T], sstr: SelectionStrategy = SelectionStrategy.FirstMatch, out: OutputStrategy = Combined) extends RDD[T](parent){
   val collector: MatchCollector[T] = new MatchCollector()
   val engine: CEPEngine[T] = sstr match {
    case SelectionStrategy.FirstMatch        => new FirstMatch(nfa, collector)
    case SelectionStrategy.AllMatches        => new AnyMatch(nfa, collector)
    case SelectionStrategy.NextMatches       => new NextMatch(nfa, collector)
    case SelectionStrategy.ContiguityMatches => new ContiguityMatch(nfa, collector)
    case _                                   => throw new Exception("The Strategy is not supported")

  }
  override def compute(split: Partition, context: TaskContext): Iterator[T] =  {
    firstParent[T].iterator(split, context).foreach (event => engine.runEngine(event))
    collector.convertEventsToArray().iterator
  }
  

  override protected def getPartitions: Array[Partition] = firstParent[Event].partitions
}