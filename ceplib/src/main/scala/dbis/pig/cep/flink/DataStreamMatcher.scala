package dbis.pig.cep.flink

import scala.reflect.ClassTag
import dbis.pig.cep.nfa.NFAController
import dbis.pig.cep.engines._
import dbis.pig.cep.ops.SelectionStrategy._
import dbis.pig.cep.ops.OutputStrategy._
import dbis.pig.backends.{SchemaClass => Event}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import dbis.pig.cep.ops.MatchCollector
import dbis.pig.cep.ops.SelectionStrategy
//import org.apache.flink.api.java.operators.CustomUnaryOperation
import scala.collection.mutable.ListBuffer
//import org.apache.flink.api.java.DataSet
//import org.apache.flink.api.java.ExecutionEnvironment
import scala.collection.JavaConversions._
import org.apache.flink.streaming.api.scala._
import dbis.pig.cep.ops.EngineConf
import org.apache.flink.util.Collector


class DataStreamMatcher[T <: Event: ClassTag: TypeInformation](@transient val input: DataStream[T], nfa: NFAController[T], flinkEnv: StreamExecutionEnvironment, sstr: SelectionStrategy = SelectionStrategy.FirstMatch, out: OutputStrategy = Combined) extends EngineConf[T](nfa, sstr) with java.io.Serializable {
  object DataStreamProcess {
    def customRun(gw: GlobalWindow, ts: Iterable[T], out: Collector[T]) = {
      ts.foreach { event => engine.runEngine(event)}
      val result = collector.convertEventsToArray()
      result.foreach { res => out.collect(res) }
    }
  }
  def compute(): DataStream[T] = {
    input.windowAll(GlobalWindows.create()).apply(DataStreamProcess.customRun _)   
  }

}