
package dbis.piglet.cep.flink

import scala.reflect.ClassTag
import dbis.piglet.cep.ops.SelectionStrategy._
import dbis.piglet.cep.ops.OutputStrategy._
import dbis.piglet.cep.nfa.NFAController
import dbis.piglet.backends.{SchemaClass => Event}
import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.api.java.ExecutionEnvironment
//import org.apache.flink.api.java.DataSet
import scala.collection.JavaConversions._
import org.apache.flink.api.scala._

class CustomDataSetMatcher[T <: Event: ClassTag: TypeInformation](dataSet: DataSet[T]) {

  def matchNFA(nfa: NFAController[T], sstr: SelectionStrategy = FirstMatch, out: OutputStrategy = Combined)  = {
    // println("create a new DataSet matcher")
    val flinkEnv = dataSet.getExecutionEnvironment
    new DataSetMatcher(dataSet, nfa, flinkEnv, sstr, out).compute()
  }

}

object CustomDataSetMatcher {

  implicit def addDataSetMatcher[T <: Event: ClassTag: TypeInformation](dataSet: DataSet[T]) = {
    // println("add a custom DataSet function")
    new CustomDataSetMatcher(dataSet)
  }
}