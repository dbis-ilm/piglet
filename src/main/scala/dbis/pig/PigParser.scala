package dbis.pig

import scala.util.parsing.combinator.JavaTokenParsers

/**
 * Created by kai on 31.03.15.
 */


class PigParser extends JavaTokenParsers {
  override protected val whiteSpace = """(\s|--.*)+""".r

  class CaseInsensitiveString(str: String) {
    def ignoreCase: Parser[String] = ("""(?i)\Q""" + str + """\E""").r
  }

  implicit def pimpString(str: String): CaseInsensitiveString = new CaseInsensitiveString(str)

  def num: Parser[Int] = wholeNumber ^^ (_.toInt)
  def expr: Parser[String] = ident
  def exprList: Parser[List[String]] = repsep(expr, ",")

  def bag: Parser[String] = ident
  def fileName: Parser[String] = stringLiteral ^^ {str => str.substring(1, str.length - 1)}

  def posField: Parser[Ref] = """\$[0-9]*""".r ^^ { p => PositionalField(p.substring(1, p.length).toInt) }
  def namedField: Parser[Ref] = ident ^^ { i => NamedField(i) }
  def literalField: Parser[Ref] = (floatingPointNumber ^^ { n => Value(n) } | stringLiteral ^^ { s => Value(s) })
  def ref: Parser[Ref] = ( posField | namedField | literalField )
  def predicate: Parser[Predicate] = ref ~ ("!=" | "<=" | ">=" | "==" | "<" | ">") ~ ref ^^ {
    case a ~ op ~ b => op match {
      case "==" => Eq(a, b)
      case "!=" => Neq(a, b)
      case "<" => Lt(a, b)
      case "<=" => Leq(a, b)
      case ">" => Gt(a, b)
      case ">=" => Geq(a, b)
    }
  }

  lazy val loadKeyword = "load".ignoreCase
  lazy val dumpKeyword = "dump".ignoreCase
  lazy val storeKeyword = "store".ignoreCase
  lazy val intoKeyword = "into".ignoreCase
  lazy val filterKeyword = "filter".ignoreCase
  lazy val byKeyword = "by".ignoreCase
  lazy val groupKeyword = "group".ignoreCase
  lazy val allKeyword = "all".ignoreCase
  lazy val joinKeyword = "join".ignoreCase
  lazy val distinctKeyword = "distinct".ignoreCase
  lazy val describeKeyword = "describe".ignoreCase
  lazy val limitKeyword = "limit".ignoreCase
  lazy val usingKeyword = "using".ignoreCase
  lazy val foreachKeyword = "foreach".ignoreCase
  lazy val generateKeyword = "generate".ignoreCase

  def usingClause: Parser[(String, List[String])] = usingKeyword ~ ident ~ "(" ~ repsep(stringLiteral, ",") ~ ")" ^^ {
    case _ ~ loader ~ _ ~ params ~ _ => (loader, params)
  }
  def loadStmt: Parser[PigOperator] = bag ~ "=" ~ loadKeyword ~ fileName ~ (usingClause?) ^^ {
    case b ~ _ ~ _ ~ f ~ u => u match {
      case Some(p) => Load(b, f, p._1, if (p._2.isEmpty) null else p._2)
      case None => Load(b, f)
    }
  }

  def dumpStmt: Parser[PigOperator] = dumpKeyword ~ bag ^^ { case _ ~ b => Dump(b) }

  def storeStmt: Parser[PigOperator] = storeKeyword ~ bag ~ intoKeyword ~ fileName ^^ { case _ ~ b ~  _ ~ f => Store(b, f) }

  def foreachStmt: Parser[PigOperator] = bag ~ "=" ~ foreachKeyword ~ bag ~ generateKeyword ~ exprList ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ ex => Foreach(out, in, ex)
  }

  def filterStmt: Parser[PigOperator] = bag ~ "=" ~ filterKeyword ~ bag ~ byKeyword ~ predicate ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ pred => Filter(out, in, pred)
  }
  def describeStmt: Parser[PigOperator] = describeKeyword ~ bag ^^ { case _ ~ b => Describe(b) }

  def refList: Parser[List[Ref]] = (ref ^^ { r => List(r) } | "(" ~ repsep(ref, ",") ~ ")" ^^ { case _ ~ rlist ~ _ => rlist})
  def groupingClause: Parser[GroupingExpression] = allKeyword ^^ { s => GroupingExpression(List())} |
    (byKeyword ~ refList ^^ { case _ ~ rlist => GroupingExpression(rlist)})
  def groupingStmt: Parser[PigOperator] = bag ~ "=" ~ groupKeyword ~ bag ~ groupingClause ^^ {
    case out ~ _ ~ _ ~ in ~ grouping => Grouping(out, in, grouping) }

  def distinctStmt: Parser[PigOperator] = bag ~ "=" ~ distinctKeyword ~ bag ^^ { case out ~ _ ~ _ ~ in => Distinct(out, in) }

  def limitStmt: Parser[PigOperator] = bag ~ "=" ~ limitKeyword ~ bag ~ num ^^ { case out ~ _ ~ _ ~ in ~ num => Limit(out, in, num) }

  def joinExpr: Parser[(String, List[Ref])] = bag ~ byKeyword ~ refList ^^ { case b ~ _ ~ rlist => (b, rlist) }
  def joinExprList: Parser[List[(String, List[Ref])]] = repsep(joinExpr, ",") ^^ { case jlist => jlist }
  def extractJoinRelation(jList: List[(String, List[Ref])]): List[String] = { jList.map{ case (alias, refs) => alias } }
  def extractJoinFields(jList: List[(String, List[Ref])]): List[List[Ref]] = { jList.map{ case (alias, refs) => refs } }
  def joinStmt: Parser[PigOperator] = bag ~ "=" ~ joinKeyword ~ joinExprList ^^ {
    case out ~ _ ~ _ ~ jlist => Join(out, extractJoinRelation(jlist), extractJoinFields(jlist)) }

  def stmt: Parser[PigOperator] = (loadStmt | dumpStmt | describeStmt | foreachStmt | filterStmt | groupingStmt |
    distinctStmt | joinStmt | storeStmt | limitStmt) ~ ";" ^^ {
    case op ~ _  => op }
  def script: Parser[List[PigOperator]] = rep(stmt)
}
