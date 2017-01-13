package dbis.piglet.codegen.flink.emitter

import dbis.piglet.codegen.{ CodeEmitter, CodeGenContext, CodeGenException }
import dbis.piglet.op.Join
import dbis.piglet.codegen.scala_lang.ScalaEmitter
import dbis.piglet.op.PigOperator
import dbis.piglet.codegen.flink.FlinkHelper

class StreamJoinEmitter extends JoinEmitter {
  override def template: String = """    val <out> = <rel1><rel2,key2,kloop:{ r,k,l |<\\>
                                    |    .join(<r>).where(k => k match {case <l> => <key1>\}).equalTo(t => <k>).window(TumblingTimeWindows.of(Time.<wUnit>(<window>)))<\\>
                                    |    .apply{(t1,t2) => (t1, t2)\}
                                    |    }>
                                    |      .map{ v => v match {case  <pairs> => <class>(<fields>) }}""".stripMargin

  override def code(ctx: CodeGenContext, op: Join): String = {
    val className = op.schema match {
      case Some(s) => ScalaEmitter.schemaClassName(s.className)
      case None => ScalaEmitter.schemaClassName(op.outPipeName)
    }
    val res = op.inputs.zip(op.fieldExprs)
    val keys = res.map { case (i, k) => emitJoinKey(CodeGenContext(ctx, Map("schema" -> i.producer.schema, "tuplePrefix" -> "t")), k) }

    val extractor = FlinkHelper.emitJoinFieldList(op)

    // for more than 2 relations the key is nested and needs to be extracted
    var keyLoopPairs = List("t")
    for (k <- keys.drop(2).indices) {
      var nesting = "(t, m0)"
      for (i <- 1 to k) nesting = s"($nesting,m$i)"
      keyLoopPairs :+= nesting
    }
    if (op.timeWindow != null)
      render(Map(
        "out" -> op.outPipeName,
        "class" -> className,
        "rel1" -> op.inputs.head.name,
        "key1" -> keys.head,
        "rel2" -> op.inputs.tail.map(_.name),
        "key2" -> keys.tail,
        "kloop" -> keyLoopPairs,
        "pairs" -> extractor._1,
        "fields" -> extractor._2,
        "window" -> op.timeWindow._1,
        "wUnit" -> op.timeWindow._2.toLowerCase()))
    else
      render(Map(
        "out" -> op.outPipeName,
        "class" -> className,
        "rel1" -> op.inputs.head.name,
        "key1" -> keys.head,
        "rel2" -> op.inputs.tail.map(_.name),
        "key2" -> keys.tail,
        "kloop" -> keyLoopPairs,
        "pairs" -> extractor._1,
        "fields" -> extractor._2))
  }
}