package dbis.pig

import scala.util.parsing.combinator.JavaTokenParsers

/**
 * Created by kai on 31.03.15.
 */

class PigParser extends JavaTokenParsers {
  override protected val whiteSpace = """(\s|--.*)+""".r

  def num: Parser[Int] = wholeNumber ^^ (_.toInt)
  def expr: Parser[String] = ident
  def exprList: Parser[List[String]] = repsep(expr, ",")
  def bag: Parser[String] = ident
  def fileName: Parser[String] = stringLiteral ^^ {str => str.substring(1, str.length - 1)}

  def posField: Parser[Ref] = """\$[0-9]*""".r ^^ { p => PositionalField(p.substring(1, p.length).toInt) }
  def namedField: Parser[Ref] = ident ^^ { i => NamedField(i) }
  def literalField: Parser[Ref] = (floatingPointNumber ^^ { n => Value(n) } | stringLiteral ^^ { s => Value(s) })
  def ref: Parser[Ref] = ( posField | namedField | literalField )
  def predicate: Parser[Predicate] = ref ~ ("!=" | "<=" | ">=" | "=" | "<" | ">") ~ ref ^^ {
    case a ~ op ~ b => op match {
      case "=" => Eq(a, b)
      case "!=" => Neq(a, b)
      case "<" => Lt(a, b)
      case "<=" => Leq(a, b)
      case ">" => Gt(a, b)
      case ">=" => Geq(a, b)
    }
  }

  def loadStmt: Parser[PigOperator] = bag ~ "=" ~ "load" ~ fileName ^^ { case b ~ _ ~ _ ~ f => Load(b, f) }
  def dumpStmt: Parser[PigOperator] = "dump" ~ bag ^^ { case _ ~ b => Dump(b) }
  def foreachStmt: Parser[PigOperator] = bag ~ "=" ~ "foreach" ~ bag ~ "generate" ~ exprList ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ ex => Foreach(out, in, ex)
  }
  def filterStmt: Parser[PigOperator] = bag ~ "=" ~ "filter" ~ bag ~ "by" ~ predicate ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ pred => Filter(out, in, pred)
  }

  def stmt: Parser[PigOperator] = (loadStmt | dumpStmt | foreachStmt | filterStmt) ~ ";" ^^ { case op ~ _  => op }
  def script: Parser[List[PigOperator]] = rep(stmt)
}
