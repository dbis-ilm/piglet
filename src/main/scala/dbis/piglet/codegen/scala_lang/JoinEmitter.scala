package dbis.piglet.codegen.scala_lang

import dbis.piglet.codegen.{CodeEmitter, CodeGenContext, CodeGenException}
import dbis.piglet.expr.Ref
import dbis.piglet.op.{Join, PigOperator}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set

/**
  * Created by kai on 12.12.16.
  */
class JoinEmitter extends CodeEmitter {
  override def template: String = """val <out> = <rel1>_kv.join(<rel2>_kv).map{case (k,(v,w)) => <class>(<fields>)}""".stripMargin
  def mwayJoinTemplate: String = """val <out> = <rel1>_kv<rel2: {rel | .join(<rel>_kv)}>.map{case (k,<pairs>) => <class>(<fields>)}""".stripMargin
  def keymapTemplate: String = """<rels,keys:{ rel,key |val <rel>_kv = <rel>.map(t => (<key>,t)) }>""".stripMargin

  /**
    *
    * @param ctx an object representing context information for code generation
    * @param joinExpr
    * @return
    */
  def emitJoinKey(ctx: CodeGenContext, joinExpr: List[Ref]): String = {
    if (joinExpr.size == 1)
      ScalaEmitter.emitRef(ctx, joinExpr.head)
    else
      s"Array(${joinExpr.map(e => ScalaEmitter.emitRef(ctx, e)).mkString(",")}).mkString"
  }

  override def code(ctx: CodeGenContext, node: PigOperator): String = {
    node match {
      case Join(out, _, exprs, _) => {
        if (!node.schema.isDefined)
          throw CodeGenException("schema required in JOIN")

        val rels = node.inputs
        val res = rels.zip(exprs)
        val keys = res.map { case (i, k) => emitJoinKey(CodeGenContext(ctx, Map("schema" -> i.producer.schema)), k) }

        /*
         * We don't generate key-value RDDs which we have already created and registered in joinKeyVars.
         * Thus, we build a list of 1 and 0's where 1 stands for a relation name for which we have already
         * created a _kv variable.
         */
        val joinKeyVars = if (ctx.contains("joinkeys")) ctx.params("joinkeys").asInstanceOf[Set[String]] else Set[String]()
        val duplicates = rels.map(r => if (joinKeyVars.contains(r.name)) 1 else 0)

        /*
         * Now we build lists for rels and keys by removing the elements corresponding to 1's in the duplicate
         * list.
         */
        val drels = rels.zipWithIndex.filter { r => duplicates(r._2) == 0 }.map(_._1)
        val dkeys = keys.zipWithIndex.filter { k => duplicates(k._2) == 0 }.map(_._1)

        /*
         * And finally, create the join kv vars for them...
         */
        var str = CodeEmitter.render(keymapTemplate, Map("rels" -> drels.map(_.name), "keys" -> dkeys))

        /*
         * We construct a string v._0, v._1 ... w._0, w._1 ...
         * The numbers of v's and w's are determined by the size of the input schemas.
         */
        val className = node.schema match {
          case Some(s) => ScalaEmitter.schemaClassName(s.className)
          case None => ScalaEmitter.schemaClassName(node.outPipeName)
        }

        /*
          *  ...as well as the actual join.
          */
        if (rels.length == 2) {
          val vsize = rels.head.inputSchema.get.fields.length
          val fieldList = node.schema.get.fields.zipWithIndex
            .map { case (f, i) => if (i < vsize) s"v._$i" else s"w._${i - vsize}" }.mkString(", ")

          str += render(
            Map("out" -> node.outPipeName,
              "rel1" -> rels.head.name,
              "class" -> className,
              "rel2" -> rels.tail.map(_.name),
              "fields" -> fieldList)) + "\n"
        }
        else {
          var pairs = "(v1,v2)"
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

          str += CodeEmitter.render(mwayJoinTemplate,
            Map("out" -> node.outPipeName,
              "rel1" -> rels.head.name,
              "class" -> className,
              "rel2" -> rels.tail.map(_.name),
              "pairs" -> pairs,
              "fields" -> fieldList.mkString(", "))) + "\n"
        }
        joinKeyVars += rels.head.name
        joinKeyVars ++= rels.tail.map(_.name)
        ctx.set("joinkeys", joinKeyVars)

        str
      }
      case _ => throw CodeGenException(s"unexpected operator: $node")
    }
  }

}
