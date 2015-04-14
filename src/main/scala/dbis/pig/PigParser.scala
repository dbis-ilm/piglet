package dbis.pig

import scala.util.parsing.combinator.JavaTokenParsers

/**
 * Created by kai on 31.03.15.
 */

/**
 * A parser for the (extended) Pig language.
 */
class PigParser extends JavaTokenParsers {
  override protected val whiteSpace = """(\s|--.*)+""".r

  /**
   * A helper class for supporting case-insensitive keywords.
   *
   * @param str the keyword string to be handled.
   */
  class CaseInsensitiveString(str: String) {
    def ignoreCase: Parser[String] = ("""(?i)\Q""" + str + """\E""").r
  }

  implicit def pimpString(str: String): CaseInsensitiveString = new CaseInsensitiveString(str)

  def num: Parser[Int] = wholeNumber ^^ (_.toInt)

  def bag: Parser[String] = ident
  def fileName: Parser[String] = stringLiteral ^^ {str => str.substring(1, str.length - 1)}

  /*
   * A reference can be a named field,a positional field (e.g $0, $1, ...) or a literal.
   */
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


  def arithmExpr: Parser[ArithmeticExpr] = term ~ rep("+" ~ term | "-" ~ term) ^^ {
    case l ~ list => list.foldLeft(l) {
      case (x, "+" ~ i) => Add(x,i)
      case (x, "-" ~ i) => Minus(x,i)
    }
  }

  def term: Parser[ArithmeticExpr] = factor ~ rep("*" ~ factor | "/" ~ factor) ^^ {
    case l ~ list => list.foldLeft(l) {
      case (x, "*" ~ i) => Mult(x,i)
      case (x, "/" ~ i) => Div(x,i)
    }
  }

  def factor: Parser[ArithmeticExpr] =  (
    "(" ~ arithmExpr ~ ")" ^^ { case _ ~ e ~ _ => e }
      | func
      | refExpr
    )

  def func: Parser[ArithmeticExpr] = ident ~ "(" ~ repsep(arithmExpr, ",") ~ ")" ^^ { case f ~ _ ~ p ~ _ => Func(f, p) }
  def refExpr: Parser[ArithmeticExpr] = ref ^^ { r => RefExpr(r) }


  /*
   * The list of case-insensitive keywords we want to accept.
   */
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
  lazy val asKeyword = "as".ignoreCase

  /*
   * <A> = LOAD <B> "<FileName>" USING <StorageFunc> (<OptParameters>)
   */
  def usingClause: Parser[(String, List[String])] = usingKeyword ~ ident ~ "(" ~ repsep(stringLiteral, ",") ~ ")" ^^ {
    case _ ~ loader ~ _ ~ params ~ _ => (loader, params)
  }

  def loadStmt: Parser[PigOperator] = bag ~ "=" ~ loadKeyword ~ fileName ~ (usingClause?) ^^ {
    case b ~ _ ~ _ ~ f ~ u => u match {
      case Some(p) => Load(b, f, p._1, if (p._2.isEmpty) null else p._2)
      case None => Load(b, f)
    }
  }

  /*
   * DUMP <A>
   */
  def dumpStmt: Parser[PigOperator] = dumpKeyword ~ bag ^^ { case _ ~ b => Dump(b) }

  /*
   * STORE <A> INTO "<FileName>"
   */
  def storeStmt: Parser[PigOperator] = storeKeyword ~ bag ~ intoKeyword ~ fileName ^^ { case _ ~ b ~  _ ~ f => Store(b, f) }

  /*
   * <A> = FOREACH <B> GENERATE <Expr> [ AS <Schema> ]
   */
  def schema: Parser[String] = ident // TODO: not only a typename but a schema!
  def exprSchema: Parser[String] = asKeyword ~ schema ^^ { case _ ~ t => t }
  def genExpr: Parser[GeneratorExpr] = arithmExpr ~ (exprSchema?) ^^ { case e ~ s => GeneratorExpr(e, s) }
  def generatorList: Parser[List[GeneratorExpr]] = repsep(genExpr, ",")
  def foreachStmt: Parser[PigOperator] = bag ~ "=" ~ foreachKeyword ~ bag ~ generateKeyword ~ generatorList ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ ex => Foreach(out, in, ex)
  }

  /*
   * <A> = FILTER <B> BY <Predicate>
   */
  def filterStmt: Parser[PigOperator] = bag ~ "=" ~ filterKeyword ~ bag ~ byKeyword ~ predicate ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ pred => Filter(out, in, pred)
  }

  /*
   * DESCRIBE <A>
   */
  def describeStmt: Parser[PigOperator] = describeKeyword ~ bag ^^ { case _ ~ b => Describe(b) }

  /*
   * <A> = GROUP <B> ALL
   * <A> = GROUP <B> BY <Ref>
   * <A> = GROUP <B> B> ( <ListOfRefs> )
   */
  def refList: Parser[List[Ref]] = (ref ^^ { r => List(r) } | "(" ~ repsep(ref, ",") ~ ")" ^^ { case _ ~ rlist ~ _ => rlist})
  def groupingClause: Parser[GroupingExpression] = allKeyword ^^ { s => GroupingExpression(List())} |
    (byKeyword ~ refList ^^ { case _ ~ rlist => GroupingExpression(rlist)})
  def groupingStmt: Parser[PigOperator] = bag ~ "=" ~ groupKeyword ~ bag ~ groupingClause ^^ {
    case out ~ _ ~ _ ~ in ~ grouping => Grouping(out, in, grouping) }

  /*
   * <A> = DISTINCT <B>
   */
  def distinctStmt: Parser[PigOperator] = bag ~ "=" ~ distinctKeyword ~ bag ^^ { case out ~ _ ~ _ ~ in => Distinct(out, in) }

  /*
   * <A> = LIMIT <B> <Num>
   */
  def limitStmt: Parser[PigOperator] = bag ~ "=" ~ limitKeyword ~ bag ~ num ^^ { case out ~ _ ~ _ ~ in ~ num => Limit(out, in, num) }

  /*
   * <A> = JOIN <B> BY <Ref>, <C> BY <Ref>, ...
   * <A> = JOIN <B> BY ( <ListOfRefs> ), <C> BY ( <ListOfRefs>), ...
   */
  def joinExpr: Parser[(String, List[Ref])] = bag ~ byKeyword ~ refList ^^ { case b ~ _ ~ rlist => (b, rlist) }
  def joinExprList: Parser[List[(String, List[Ref])]] = repsep(joinExpr, ",") ^^ { case jlist => jlist }
  def extractJoinRelation(jList: List[(String, List[Ref])]): List[String] = { jList.map{ case (alias, refs) => alias } }
  def extractJoinFields(jList: List[(String, List[Ref])]): List[List[Ref]] = { jList.map{ case (alias, refs) => refs } }
  def joinStmt: Parser[PigOperator] = bag ~ "=" ~ joinKeyword ~ joinExprList ^^ {
    case out ~ _ ~ _ ~ jlist => Join(out, extractJoinRelation(jlist), extractJoinFields(jlist)) }

  /*
   * A statement can be one of the above delimited by a semicolon.
   */
  def stmt: Parser[PigOperator] = (loadStmt | dumpStmt | describeStmt | foreachStmt | filterStmt | groupingStmt |
    distinctStmt | joinStmt | storeStmt | limitStmt) ~ ";" ^^ {
    case op ~ _  => op }

  /*
   * A script is a list of statements.
   */
  def script: Parser[List[PigOperator]] = rep(stmt)
}
