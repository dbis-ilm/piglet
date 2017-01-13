package dbis.piglet.cep.flink

import scala.reflect.ClassTag
import dbis.piglet.cep.nfa.NFAController
import dbis.piglet.cep.engines._
import dbis.piglet.cep.ops.SelectionStrategy._
import dbis.piglet.cep.ops.OutputStrategy._
import dbis.piglet.backends.{SchemaClass => Event}
import dbis.piglet.cep.ops.MatchCollector
import org.apache.flink.api.common.typeinfo.TypeInformation
import dbis.piglet.cep.ops.SelectionStrategy
//import org.apache.flink.api.java.operators.CustomUnaryOperation
//import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
//import org.apache.flink.api.java.DataSet
//import org.apache.flink.api.java.ExecutionEnvironment
import scala.collection.JavaConversions._
import org.apache.flink.api.scala._
import dbis.piglet.cep.ops.EngineConf

class DataSetMatcher[T <: Event: ClassTag: TypeInformation](input: DataSet[T], nfa: NFAController[T], flinkEnv: ExecutionEnvironment, sstr: SelectionStrategy = SelectionStrategy.FirstMatch, out: OutputStrategy = Combined) extends EngineConf[T](nfa, sstr) with java.io.Serializable {
  def compute(): DataSet[T] = {
   input.collect().foreach ( event => engine.runEngine(event)  )
   flinkEnv.fromCollection(collector.convertEventsToArray().toSeq)
  }

}