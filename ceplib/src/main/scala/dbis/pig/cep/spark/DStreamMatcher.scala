package dbis.pig.cep.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.Time
import scala.reflect.ClassTag
import dbis.pig.cep.ops.SelectionStrategy._
import dbis.pig.cep.ops.OutputStrategy._
import dbis.pig.cep.nfa.NFAController
import dbis.pig.cep.engines._
import dbis.pig.backends.{SchemaClass => Event}
import dbis.pig.cep.ops.MatchCollector
import dbis.pig.cep.ops.SelectionStrategy

class DStreamMatcher[T <: Event: ClassTag](parent: DStream[T], nfa: NFAController[T], sstr: SelectionStrategy = SelectionStrategy.FirstMatch, out: OutputStrategy = Combined) extends DStream[T](parent.context) {

   val collector: MatchCollector[T] = new MatchCollector()
   val engine: CEPEngine[T] = sstr match {
    case SelectionStrategy.FirstMatch        => new FirstMatch(nfa, collector)
    case SelectionStrategy.AllMatches        => new AnyMatch(nfa, collector)
    case SelectionStrategy.NextMatches       => new NextMatch(nfa, collector)
    case SelectionStrategy.ContiguityMatches => new ContiguityMatch(nfa, collector)
    case _                                   => throw new Exception("The Strategy is not supported")

  }
  /** Time interval after which the DStream generates a RDD */
  override def slideDuration = parent.slideDuration

  /** List of parent DStreams on which this DStream depends on */
  override def dependencies: List[DStream[_]] = List(parent)

  /** Method that generates a RDD for the given time */
  override def compute(validTime: Time): Option[RDD[T]] = {
    println("The matcher receive an event")
    parent.compute(validTime) match {
      case Some(rdd) => rdd.foreach(event => engine.runEngine(event))
      case None      => null
    }
    parent.compute(validTime)
    //val data = Array(new SalesRecord("1","2","3",4))
    //val x = Some(parent.context.sparkContext.parallelize(data))
    //println(x.get.collect().toList)
    //x
  }
}