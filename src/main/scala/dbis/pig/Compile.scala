package dbis.pig

/**
 * Created by kai on 08.04.15.
 */
trait GenCodeBase {
  def emitNode(node: PigOperator): String
  def emitHeader(scriptName: String): String
  def emitFooter: String
  def emitPredicate(schema: Option[Schema], predicate: Predicate): String
  def emitRef(schema: Option[Schema], ref: Ref): String
  def emitGrouping(schema: Option[Schema], groupingExpr: GroupingExpression): String
}

trait Compile {
  def codeGen: GenCodeBase

  def compile(scriptName: String, plan: DataflowPlan): String = {
    require(codeGen != null, "code generator undefined")
    var code = codeGen.emitHeader(scriptName)
    for (n <- plan.operators) {
      code = code + codeGen.emitNode(n) + "\n"
    }
    code + codeGen.emitFooter
  }
}
