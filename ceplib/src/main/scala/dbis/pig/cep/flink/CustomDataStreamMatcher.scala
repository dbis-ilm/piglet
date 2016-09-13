
package dbis.pig.cep.flink

import scala.reflect.ClassTag
import dbis.pig.cep.ops.SelectionStrategy._
import dbis.pig.cep.ops.OutputStrategy._
import dbis.pig.cep.nfa.NFAController
import dbis.pig.backends.{SchemaClass => Event}
import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.ExecutionEnvironment
//import org.apache.flink.api.java.DataSet
import scala.collection.JavaConversions._
import org.apache.flink.streaming.api.scala._

class CustomDataStreamMatcher[T <: Event: ClassTag: TypeInformation](@transient val dataStream: DataStream[T]) {

  def matchNFA(nfa: NFAController[T], flinkEnv: StreamExecutionEnvironment, sstr: SelectionStrategy = FirstMatch, out: OutputStrategy = Combined)  = {
    // println("create a new DataStream matcher")
    new DataStreamMatcher(dataStream, nfa, flinkEnv, sstr, out).compute()
  }

}

object CustomDataStreamMatcher {

  implicit def addDataSetMatcher[T <: Event: ClassTag: TypeInformation](@transient dataStream: DataStream[T]) = {
    // println("add a custom DataStream function")
    new CustomDataStreamMatcher(dataStream)
  }
}