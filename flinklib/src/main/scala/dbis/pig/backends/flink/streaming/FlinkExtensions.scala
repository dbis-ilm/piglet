package dbis.pig.backends.flink.streaming

import org.apache.flink.streaming.api.scala.WindowedDataStream
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector


object FlinkExtensions {
  implicit class EnhancedWindowedDataStream(val w: WindowedDataStream[List[Any]]) {

    def distinct = w.mapWindow(distinctMapFunction _)

    private def distinctMapFunction(ts: Iterable[List[Any]], out: Collector[List[Any]]) ={
      ts.toList.distinct.foreach{ x => out.collect(x) }
    }
  }

  implicit class EnhancedWindowedDataStreamString(val w: WindowedDataStream[List[String]]) {

    def distinct = w.mapWindow(distinctMapFunction _)

    private def distinctMapFunction(ts: Iterable[List[String]], out: Collector[List[String]]) ={
      ts.toList.distinct.foreach{ x => out.collect(x) }
    }   
  }
}
