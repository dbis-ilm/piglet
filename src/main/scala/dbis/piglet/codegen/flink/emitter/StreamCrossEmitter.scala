package dbis.piglet.codegen.flink.emitter

import dbis.piglet.codegen.{ CodeEmitter, CodeGenContext, CodeGenException }
import dbis.piglet.codegen.scala_lang.CrossEmitter
import dbis.piglet.codegen.scala_lang.ScalaEmitter
import dbis.piglet.op.Cross
import dbis.piglet.codegen.flink.FlinkHelper

class StreamCrossEmitter extends CrossEmitter {
  override def template: String = """    val <out> = <rel1><rel2:{ r |<\\>
                                    |    .map(t => (1,t)).join(<r>.map(t => (1,t))).where(t => t._1).equalTo(t => t._1).window(TumblingTimeWindows.of(Time.<wUnit>(<window>)))<\\>
                                    |    .apply{(t1,t2) => (t1._2, t2._2)\}
                                    |    }>
                                    |      .map{ v => v match {case  <pairs> => <class>(<fields>) }}""".stripMargin

  override def code(ctx: CodeGenContext, op: Cross): String = {
   val rels = op.inputs
    val className = op.schema match {
      case Some(s) => ScalaEmitter.schemaClassName(s.className)
      case None    => ScalaEmitter.schemaClassName(op.outPipeName)
    }
    val extractor = FlinkHelper.emitJoinFieldList(op)
    val params =
      if (op.timeWindow != null)
        Map("out" -> op.outPipeName,
          "class" -> className,
          "rel1" -> rels.head.name,
          "rel2" -> rels.tail.map(_.name),
          "pairs" -> extractor._1,
          "fields" -> extractor._2,
          "window" -> op.timeWindow._1,
          "wUnit" -> op.timeWindow._2)
      else
        Map("out" -> op.outPipeName,
          "rel1" -> rels.head.name,
          "rel2" -> rels.tail.map(_.name),
          "extractor" -> extractor)
    render(params)
  }
}