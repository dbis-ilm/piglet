package dbis.piglet.codegen

import dbis.piglet.schema.Schema

object CodeGenTarget extends Enumeration {
  val Unknown, Spark, SparkStreaming, Flink, FlinkStreaming, PipeFabric = Value
}

/**
  * CodeGenContext provides a context object which is passed to the specific generator methods.
  *
  * @param params a map of key-object pairs needed for passing context information
  * @param target the id of the target platform
  * @param udfAliases a map of alias names for user-defined functions
  */
case class CodeGenContext (var params: collection.mutable.Map[String, Any],
                      target: CodeGenTarget.Value,
                      udfAliases: Option[Map[String, (String, List[Any])]] = None) {
  def apply(n: String) = params(n)

  def asBoolean(n: String) = params(n).asInstanceOf[Boolean]

  def asString(n: String) = params(n).asInstanceOf[String]

  def asInt(n: String) = params(n).asInstanceOf[Int]

  def set(n: String, v: Any) = this.params += (n -> v)

  def schema: Option[Schema] = params("schema") match {
    case None => throw new CodeGenException("unkown schema in context")
    case Some(s) => Some(s.asInstanceOf[Schema])
  }

}

object CodeGenContext {
  def apply(t: CodeGenTarget.Value,
                     aliases: Option[Map[String, (String, List[Any])]] = None) =
    new CodeGenContext(collection.mutable.Map[String, Any](), t, aliases)

  def apply(ctx: CodeGenContext) =
    new CodeGenContext(collection.mutable.Map[String, Any]() ++= ctx.params, ctx.target, ctx.udfAliases)

  def apply(ctx: CodeGenContext, m: Map[String, Any]) =
    new CodeGenContext(collection.mutable.Map[String, Any]() ++= ctx.params ++= m, ctx.target, ctx.udfAliases)

}
