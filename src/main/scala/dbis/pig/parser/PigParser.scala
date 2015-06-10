/*
 * Copyright (c) 2015 The Piglet team,
 *                    All Rights Reserved.
 *
 * This file is part of the Piglet package.
 *
 * PipeFabric is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License (GPL) as
 * published by the Free Software Foundation; either version 2 of
 * the License, or (at your option) any later version.
 *
 * This package is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; see the file LICENSE.
 * If not you can find the GPL at http://www.gnu.org/copyleft/gpl.html
 */
package dbis.pig.parser

import dbis.pig._
import dbis.pig.op._
import dbis.pig.schema._

import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.parsing.input.CharSequenceReader

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

  def pigStringLiteral: Parser[String] =
    ("'"+"""([^'\p{Cntrl}\\]|\\[\\"bfnrt]|\\u[a-fA-F0-9]{4})*"""+"'").r

  def unquote(s: String): String = s.substring(1, s.length - 1)

  def num: Parser[Int] = wholeNumber ^^ (_.toInt)

  def bag: Parser[String] = ident
  def fileName: Parser[String] = pigStringLiteral ^^ { str => unquote(str) }

  def className: Parser[String] = repsep(ident, ".") ^^ { identList => identList.mkString(".")}

  /*
   * A reference can be a named field, a positional field (e.g $0, $1, ...) or a literal.
   */
  def posField: Parser[Ref] = """\$[0-9]*""".r ^^ { p => PositionalField(p.substring(1, p.length).toInt) }
  def namedField: Parser[Ref] = ident ^^ { i => NamedField(i) }
  def literalField: Parser[Ref] = (floatingPointNumber ^^ { n => Value(n) } | stringLiteral ^^ { s => Value(s) })
  def fieldSpec: Parser[Ref] = (posField | namedField | literalField)
  /*
   * It can be also a dereference operator for tuples, bags or maps.
   */
  def derefBagOrTuple: Parser[Ref] = (posField | namedField) ~ "." ~ (posField | namedField) ^^ { case r1 ~ _ ~ r2 => DerefTuple(r1, r2) }
  def derefMap: Parser[Ref] = (posField | namedField) ~ "#" ~ stringLiteral ^^ { case m ~ _ ~ k => DerefMap(m, k) }

  def ref: Parser[Ref] = (derefMap |  derefBagOrTuple | fieldSpec) // ( fieldSpec | derefBagOrTuple | derefMap)

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

  // def typeName: Parser[String] = ( "int" | "float" | "double" | "chararray"| " bytearray") ^^ { s => s }

  def castTypeSpec: Parser[PigType] = (
    "int" ^^ { _ => Types.IntType }
      | "long" ^^ { _ => Types.LongType }
      | "float" ^^ { _ => Types.FloatType }
      | "double" ^^ { _ => Types.DoubleType }
      | "boolean" ^^ { _ => Types.BooleanType }
      | "chararray" ^^ { _ => Types.CharArrayType }
      | "bytearray" ^^{ _ => Types.ByteArrayType }
      | "tuple" ~ "(" ~ repsep(castTypeSpec, ",") ~ ")" ^^{
            case _ ~ _ ~ typeList ~ _ => TupleType("", typeList.map(t => Field("", t)).toArray)
        }
      /*
       * bag schema: bag{tuple(<list of types>)}
       */
     // | "bag" ~ "{" ~ "tuple" ~ "(" ~ ")" ~ repsep(castTypeSpec, ",") ~ "}" ^^{ case _ ~ "{" ~ tup ~ "}" => BagType("", tup) }
      /*
       * map schema: map[<list of types>]
       */
      | "map" ~ "[" ~ "]" ^^{ case _ ~ _ ~ _ => MapType("", Types.ByteArrayType) }
    )

  def factor: Parser[ArithmeticExpr] =  (
     "(" ~ castTypeSpec ~ ")" ~ refExpr ^^ { case _ ~ t ~ _ ~ e => CastExpr(t, e) }
      | "(" ~ arithmExpr ~ ")" ^^ { case _ ~ e ~ _ => e }
      | func
      | refExpr
    )

  def func: Parser[ArithmeticExpr] = className ~ "(" ~ repsep(arithmExpr, ",") ~ ")" ^^ { case f ~ _ ~ p ~ _ => Func(f, p) }
  def refExpr: Parser[ArithmeticExpr] = ref ^^ { r => RefExpr(r) }

  def comparisonExpr: Parser[Predicate] = arithmExpr ~ ("!=" | "<=" | ">=" | "==" | "<" | ">") ~ arithmExpr ^^ {
    case a ~ op ~ b => op match {
      case "==" => Eq(a, b)
      case "!=" => Neq(a, b)
      case "<" => Lt(a, b)
      case "<=" => Leq(a, b)
      case ">" => Gt(a, b)
      case ">=" => Geq(a, b)
    }
  }

  def logicalTerm: Parser[Predicate] = (
    comparisonExpr ^^ { e => e }
     | "(" ~ logicalExpr ~ ")" ^^ { case _ ~ e ~ _ => e }
    )

  def logicalExpr: Parser[Predicate] = (
      logicalTerm ~ (andKeyword | orKeyword) ~ logicalTerm ^^ {
        case a ~ op ~ b => op match {
          case "AND" => And(a, b)
          case "OR" => Or(a, b)
        }
      }
      | notKeyword ~ logicalTerm ^^ { case _ ~ e => Not(e) }
      | logicalTerm ^^ { e => e }
     )

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
  lazy val unionKeyword = "union".ignoreCase
  lazy val registerKeyword = "register".ignoreCase
  lazy val streamKeyword = "stream".ignoreCase
  lazy val throughKeyword = "through".ignoreCase
  lazy val sampleKeyword = "sample".ignoreCase
  lazy val orderKeyword = "order".ignoreCase
  lazy val ascKeyword = "asc".ignoreCase
  lazy val descKeyword = "desc".ignoreCase
  lazy val andKeyword = "and".ignoreCase
  lazy val orKeyword = "or".ignoreCase
  lazy val notKeyword = "not".ignoreCase
  lazy val toKeyword = "to".ignoreCase
  lazy val zmqSubscriberKeyword = "zmq_subscriber".ignoreCase
  lazy val zmqPublisherKeyword = "zmq_publisher".ignoreCase
  /*
   * tuple schema: tuple(<list of fields>) or (<list of fields>)
   */
  def tupleTypeSpec: Parser[TupleType] =
    ("tuple"?) ~ "(" ~repsep(fieldSchema, ",") ~ ")" ^^{ case _ ~ _ ~ fieldList ~ _ => TupleType("", fieldList.toArray) }

  def typeSpec: Parser[PigType] = (
    "int" ^^ { _ => Types.IntType }
    | "long" ^^ { _ => Types.LongType }
    | "float" ^^ { _ => Types.FloatType }
    | "double" ^^ { _ => Types.DoubleType }
    | "boolean" ^^ { _ => Types.BooleanType }
    | "chararray" ^^ { _ => Types.CharArrayType }
    | "bytearray" ^^{ _ => Types.ByteArrayType }
    | tupleTypeSpec
      /*
       * bag schema: bag{<tuple>} or {<tuple>}
       */
    | ("bag"?) ~ "{" ~ ident ~ ":" ~ tupleTypeSpec ~ "}" ^^{ case _ ~ _ ~ id ~ _ ~ tup ~ _ => tup.name = id; BagType("", tup) }
      /*
       * map schema: map[<list of fields>] or [<list of fields>]
       */
    | ("map"?) ~ "[" ~(typeSpec?) ~ "]" ^^{ case _ ~ _ ~ ty ~ _ => ty match {
        case Some(t) => MapType("", t)
        case None => MapType("", Types.ByteArrayType)
    }}
    )

  /*
   * schema of a field: <identifier> : type or simply <identifier>
   */
  def fieldType: Parser[PigType] = ":" ~ typeSpec ^^ { case _ ~ t => t }
  def fieldSchema: Parser[Field] = ident ~ (fieldType?) ^^ {
    case n ~ t => t match {
      case Some(tp) => Field(n, tp)
      case None => Field(n, Types.ByteArrayType)
    }
  }
  /*
   * <A> = LOAD <B> "<FileName>" USING <StorageFunc> (<OptParameters>) [ AS (<Schema>) ]
   */
  def loadSchemaClause: Parser[Schema] = asKeyword ~ "(" ~ repsep(fieldSchema, ",") ~ ")" ^^{
    case _ ~ _ ~ fieldList ~ _ => Schema(BagType("", TupleType("", fieldList.toArray)))
  }

  def usingClause: Parser[(String, List[String])] = usingKeyword ~ ident ~ "(" ~ repsep(pigStringLiteral, ",") ~ ")" ^^ {
    case _ ~ loader ~ _ ~ params ~ _ => (loader, params)
  }

  def loadStmt: Parser[PigOperator] = bag ~ "=" ~ loadKeyword ~ fileName ~ (usingClause?) ~ (loadSchemaClause?) ^^ {
    case b ~ _ ~ _ ~ f ~ u ~ s => u match {
        case Some(p) => Load(b, f, s, p._1, if (p._2.isEmpty) null else p._2)
        case None => Load(b, f, s)
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
  def exprSchema: Parser[Field] = asKeyword ~ fieldSchema ^^ { case _ ~ t => t }
  def genExpr: Parser[GeneratorExpr] = arithmExpr ~ (exprSchema?) ^^ { case e ~ s => GeneratorExpr(e, s) }
  def generatorList: Parser[List[GeneratorExpr]] = repsep(genExpr, ",")
  def foreachStmt: Parser[PigOperator] = bag ~ "=" ~ foreachKeyword ~ bag ~ generateKeyword ~ generatorList ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ ex => Foreach(out, in, ex)
  }

  /*
   * <A> = FILTER <B> BY <Predicate>
   */
  def filterStmt: Parser[PigOperator] = bag ~ "=" ~ filterKeyword ~ bag ~ byKeyword ~ logicalExpr ^^ {
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
   * <A> = UNION <B>, <C>, <D>, ...
   */
  def unionStmt: Parser[PigOperator] = bag ~ "=" ~ unionKeyword ~ repsep(bag, ",") ^^ { case out ~ _ ~ _ ~ rlist => Union(out, rlist)}

  /*
   * REGISTER <JarFile>
   */
  def registerStmt: Parser[PigOperator] = registerKeyword ~ stringLiteral ^^{ case _ ~ uri => Register(uri) }

  /*
   * <A> = STREAM <B> TROUGH <Operator> [(ParamList)] [AS (<Schema>) ]
   */
  def paramList: Parser[List[Ref]] = "(" ~ repsep(ref, ",") ~ ")" ^^ { case _ ~ rlist ~ _ => rlist}

  def streamStmt: Parser[PigOperator] = bag ~ "=" ~ streamKeyword ~ bag ~ throughKeyword ~ className ~ (paramList?) ~ (loadSchemaClause?) ^^{
    case out ~ _ ~_ ~ in ~ _ ~ opname ~ params ~ schema => StreamOp(out, in, opname, params, schema)
  }

  /*
   * <A> = SAMPLE <B> <num>
   * <A> = SAMPLE <B> <Expr>
   */
  def sampleStmt: Parser[PigOperator] = bag ~ "=" ~ sampleKeyword ~ bag ~ arithmExpr ^^ {
    case out ~ _ ~ _ ~ in ~ expr => Sample(out, in, expr)
  }

  /*
   * * [ASC | DESC]
   * field [ASC | DESC], field [ASC | DESC]
   */
  import OrderByDirection._

  def sortOrder: Parser[OrderByDirection] = ascKeyword ^^ { _ => OrderByDirection.AscendingOrder } |
    descKeyword ^^ { _ => OrderByDirection.DescendingOrder }

  def allOrderSpec: Parser[OrderBySpec] = "*" ~ (sortOrder?) ^^ { case s ~ o => o match {
     case Some(dir) => OrderBySpec(Value("*"), dir)
      case None => OrderBySpec(Value("*"), OrderByDirection.AscendingOrder)
    }
  }

  def fieldOrderSpec:Parser[OrderBySpec] =  (posField | namedField) ~ (sortOrder?) ^^ {
    case r ~ o => o match {
      case Some(dir) => OrderBySpec(r, dir)
      case None => OrderBySpec(r, OrderByDirection.AscendingOrder)
    }
  }

  def orderSpec: Parser[List[OrderBySpec]] = ( allOrderSpec ^^ {s => List(s)} | repsep(fieldOrderSpec, ",") )

  /*
   * <A> = ORDER <B> BY <OrderSpec>
   */
  def orderByStmt: Parser[PigOperator] = bag ~ "=" ~ orderKeyword ~ bag ~ byKeyword ~ orderSpec ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ spec => OrderBy(out, in, spec)
  }

  /*
   * <A> = ZMQ_SUBSCRIBER <address> [ AS <Schema> ]
   */
  def ipMember: Parser[String] = "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)".r 
  def ipv4: Parser[String] = ipMember ~ "." ~ ipMember ~ "." ~ ipMember ~ "." ~ ipMember ^^{
    case i1 ~ _ ~ i2 ~ _ ~ i3 ~ _ ~i4 => i1 + "." + i2 + "." + i3 + "." + i4
  }
  def port: Parser[String] = ":([0-9]{1,5})".r
  def bindAddress: Parser[String] = (ipv4 | "*" | ident) ~ (port | "*") ^^ { case ip ~ p => ip + p }
  def tcpSocket: Parser[String] = "tcp://" ~ bindAddress ^^ { case trans ~ addr => trans + addr}
  def ipcSocket: Parser[String] = "ipc://" ~ (fileName | "*") ^^ { case trans ~  path => trans + path}
  def inprocSocket: Parser[String] = "inproc://" ~ ident ^^ { case trans ~ name => trans + name}
  def pgmSocket: Parser[String] = ("pgm://" | "epgm://") ~ (ipv4 | ident) ~ ";" ~ ipv4 ~ port ^^ { 
      case trans ~ interface ~ _ ~ ip ~ p => trans + interface + ";" + ip + p
    }
  //def transports: Parser[String] = "(inproc|ipc|tcp|pgm|epgm)".r
  def zmqAddress: Parser[String] = "'" ~ (tcpSocket | ipcSocket | inprocSocket) ~ "'" ^^ { case _ ~ addr ~ _ => addr}
  def zmqSubscriberStmt: Parser[PigOperator] = bag ~ "=" ~ zmqSubscriberKeyword ~ zmqAddress ~ (loadSchemaClause?) ^^ {
    case out ~ _ ~ _ ~ addr ~ schema => ZmqSubscriber(out, addr, schema)
  }

  /*
   * ZMQ_PUBLISHER <A> TO <address>
   */
  def zmqPublisherStmt: Parser[PigOperator] = zmqPublisherKeyword ~ bag ~ toKeyword ~ zmqAddress ^^ {
    case _ ~ b ~ _ ~ addr => ZmqPublisher(b, addr)
  }

  /*
   * A statement can be one of the above delimited by a semicolon.
   */
  def stmt: Parser[PigOperator] = (loadStmt | dumpStmt | describeStmt | foreachStmt | filterStmt | groupingStmt |
    distinctStmt | joinStmt | storeStmt | limitStmt | unionStmt | registerStmt | streamStmt | sampleStmt | orderByStmt |
    zmqSubscriberStmt | zmqPublisherStmt) ~ ";" ^^ {
    case op ~ _  => op }

  /*
   * A script is a list of statements.
   */
  def script: Parser[List[PigOperator]] = rep(stmt)

  def parseScript(s: CharSequence): List[PigOperator] = {
    parseScript(new CharSequenceReader(s))
  }

  def parseScript(input: CharSequenceReader): List[PigOperator] = {
    parsePhrase(input) match {
      case Success(t, _) => t
      case NoSuccess(msg, next) => 
        throw new IllegalArgumentException(s"Could not parse input string:\n${next.pos.longString} => $msg")
    }
  }

  def parsePhrase(input: CharSequenceReader): ParseResult[List[PigOperator]] = {
    phrase(script)(input)
  }

}