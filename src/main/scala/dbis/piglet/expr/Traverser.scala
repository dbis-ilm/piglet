package dbis.piglet.expr

//import dbis.piglet.op.{Func, RefExpr, Expr, NamedField}
import dbis.piglet.schema.Schema

import scala.collection.mutable.ListBuffer


class NamedFieldExtractor {
  val fields = ListBuffer[NamedField]()

  def collectNamedFields(schema: Schema, ex: Expr): Boolean = ex match {
    case RefExpr(r) => r match {
      case NamedField(n, _) => fields += r.asInstanceOf[NamedField]; true
      case _ => true
    }
    case _ => true
  }
}

class RefExprExtractor {
  val exprs = ListBuffer[RefExpr]()

  def collectRefExprs(schema: Schema, ex: Expr): Boolean = ex match {
    case RefExpr(r) => exprs += ex.asInstanceOf[RefExpr]; true
    case _ => true
  }
}

class FuncExtractor {
  val funcs = ListBuffer[Func]()

  def collectFuncExprs(schema: Schema, ex: Expr): Boolean = ex match {
    case Func(f, params) => funcs += ex.asInstanceOf[Func]; true
    case _ => true
  }
}

