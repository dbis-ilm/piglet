package dbis.piglet.codegen.flink

import dbis.piglet.codegen.CodeGenException
import dbis.piglet.expr.NamedField
import dbis.piglet.expr.PositionalField
import dbis.piglet.schema.Schema
import dbis.piglet.expr.Ref
import dbis.piglet.op.PigOperator
import scala.collection.mutable.ArrayBuffer

object FlinkHelper {
  def getOrderIndex(schema: Option[Schema],
    ref: Ref): Int = schema match {

    case Some(s) => ref match {
      case nf @ NamedField(f, _) => s.indexOfField(nf)
      case PositionalField(pos) => pos
      case _ => 0
    }
    case None => throw new CodeGenException(s"the Flink OrderBy/Join operator needs a schema, thus, invalid field ")
  }
  /**
   * Determines the resulting field list and joined pairs for cross and join operators.
   *
   * @param node the Join or Cross node
   * @return the pairs and fields as a Tuple2[String, String]
   */
  def emitJoinFieldList(node: PigOperator): (String, String) = {
    val rels = node.inputs
    var fields = ""
    var pairs = "(v,w)"
    if (rels.length == 2) {
      val vsize = rels.head.inputSchema.get.fields.length
      fields = node.schema.get.fields.zipWithIndex
        .map { case (f, i) => if (i < vsize) s"v._$i" else s"w._${i - vsize}" }.mkString(", ")
    } else {
      pairs = "(v1,v2)"
      for (i <- 3 to rels.length) {
        pairs = s"($pairs,v$i)"
      }
      val fieldList = ArrayBuffer[String]()
      for (i <- 1 to node.inputs.length) {
        node.inputs(i - 1).producer.schema match {
          case Some(s) => fieldList ++= s.fields.zipWithIndex.map { case (f, k) => s"v$i._$k" }
          case None => fieldList += s"v$i._0"
        }
      }
      fields = fieldList.mkString(", ")
    }
    (pairs, fields)
  }

  def printQuote(values: List[String]) = """"""" + values.mkString("""","""") + """""""
}