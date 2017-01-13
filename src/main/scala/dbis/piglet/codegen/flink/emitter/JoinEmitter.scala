package dbis.piglet.codegen.flink.emitter

import dbis.piglet.codegen.{ CodeEmitter, CodeGenContext, CodeGenException }
import dbis.piglet.expr.Ref
import dbis.piglet.op.Join

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set
import dbis.piglet.codegen.scala_lang.ScalaEmitter
import scala.collection.mutable.ListBuffer
import dbis.piglet.codegen.flink.FlinkHelper

class JoinEmitter extends dbis.piglet.codegen.scala_lang.JoinEmitter {
  override def template: String = """    val <out> = <rel1><rels, rel1_keys, rel2_keys:{ r,k1, k2 | .join(<r>).where(<k1>).equalTo(<k2>)}>.map{ 
                                    |      t => 
                                    |        val <pairs> = t
                                    |        <class>(<fields>)
                                    |    }""".stripMargin

  override def code(ctx: CodeGenContext, op: Join): String = {
    if (!op.schema.isDefined)
      throw CodeGenException("schema required in JOIN")

    val res = op.inputs.zip(op.fieldExprs)
    val keys = res.map { case (i, k) => k.map { x => s"_${FlinkHelper.getOrderIndex(i.producer.schema, x)}" } }
    var keysGroup: ListBuffer[(List[String], List[String])] = new ListBuffer
    for (i <- 0 until keys.length - 1) {
      val v = (keys(i), keys(i + 1))
      keysGroup += v
    }
    val keysGroup1 = keysGroup.zipWithIndex.map {
      case (i, k) =>
        if (k > 0)
          (FlinkHelper.printQuote(i._1.map { x => s"_$k.$x" }), FlinkHelper.printQuote(i._2))
        else
          (FlinkHelper.printQuote(i._1), FlinkHelper.printQuote(i._2))
    }
    val keys1 = keysGroup1.map(x => x._1)
    val keys2 = keysGroup1.map(x => x._2)

    val className = op.schema match {
      case Some(s) => ScalaEmitter.schemaClassName(s.className)
      case None => ScalaEmitter.schemaClassName(op.outPipeName)
    }
    var pairs = "(v1,v2)"
    for (i <- 3 to op.inputs.length) {
      pairs = s"($pairs,v$i)"
    }
    val fieldList = ArrayBuffer[String]()
    for (i <- 1 to op.inputs.length) {
      op.inputs(i - 1).producer.schema match {
        case Some(s) => fieldList ++= s.fields.zipWithIndex.map { case (f, k) => s"v$i._$k" }
        case None => fieldList += s"v$i._0"
      }
    }
    render(
      Map("out" -> op.outPipeName,
        "rel1" -> op.inputs.head.name,
        "class" -> className,
        "rels" -> op.inputs.tail.map(_.name),
        "pairs" -> pairs,
        "rel1_keys" -> keys1,
        "rel2_keys" -> keys2,
        "fields" -> fieldList.mkString(", ")))
  }
}