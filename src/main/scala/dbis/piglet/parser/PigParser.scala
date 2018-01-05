/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dbis.piglet.parser

import java.net.URI
import java.util.NoSuchElementException

import dbis.piglet.expr._
import dbis.piglet.op._
import dbis.piglet.op.cmd._
import dbis.piglet.schema._
import dbis.piglet.tools.HdfsCommand
import dbis.piglet.tools.logging.PigletLogging

import scala.language.{implicitConversions, postfixOps}
import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.parsing.input.CharSequenceReader

/**
 * A parser for the (extended) Pig language.
 */
class PigParser extends JavaTokenParsers with PigletLogging {

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

  def unquote(s: String): String = if(s.startsWith("'") && s.endsWith("'")) s.substring(1, s.length - 1) else s

  def num: Parser[Int] = wholeNumber ^^ (_.toInt)

  def bag: Parser[String] = """\$?[a-zA-Z_]\w*""".r
  def fileName: Parser[String] = pigStringLiteral ^^ { str => unquote(str) }

  def className: Parser[String] = repsep(ident, ".") ^^ { identList => identList.mkString(".")}

  /*
   * A reference can be a named field, a positional field (e.g $0, $1, ...) or a literal.
   */
  def posField: Parser[Ref] = """\$[0-9]*""".r ^^ { p => PositionalField(p.substring(1, p.length).toInt) }
  /*
   * A NamedField is either just the field name or the field name with lineage information prepended.
   */
  def namedField: Parser[Ref] = not(boolean) ~>  (namedFieldWithLineage | namedFieldWithoutLineage)
  def namedFieldWithoutLineage: Parser[NamedField] =  bag ^^ (i => NamedField(i))
  def namedFieldWithLineage: Parser[NamedField] = rep1sep(bag, Field.lineageSeparator) ^^
    (l => NamedField.fromStringList(l))

  /*
   * A constant is either a floating point number, an integer, a string literal or a boolean value.
   */
  def literalField: Parser[Ref] =
    floatingPointNumber ^^ { n => if (n.contains('.')) Value(n.toDouble) else Value(n.toInt) } |
      wholeNumber ^^ { i => Value(i.toInt) } |
      stringLiteral ^^ { s => Value(s) } |
    boolean ^^ { b => Value(b) }

  def fieldSpec: Parser[Ref] = namedField | posField | literalField
  /*
   * A reference can be also a dereference operator for tuples, bags or maps.
   */
  def derefBagOrTuple: Parser[Ref] = (namedField | posField) ~ "." ~ (posField | namedField) ^^ { case r1 ~ _ ~ r2 => DerefTuple(r1, r2) }
  def derefMap: Parser[Ref] = (namedField | posField) ~ "#" ~ stringLiteral ^^ { case m ~ _ ~ k => DerefMap(m, k) }

  def ref: Parser[Ref] = derefMap | derefBagOrTuple | fieldSpec // ( fieldSpec | derefBagOrTuple | derefMap)

  def arithmExpr: Parser[ArithmeticExpr] = term ~ rep("+" ~ term | "-" ~ term) ^^ {
    case l ~ list => list.foldLeft(l) {
      case (x, "+" ~ i) => Add(x,i)
      case (x, "-" ~ i) => Minus(x,i)
    }
  }

//  literalField ^^ { f => RefExpr(f) }
//    | func ^^ { f => f }


  def term: Parser[ArithmeticExpr] = factor ~ rep("*" ~ factor | "/" ~ factor) ^^ {
    case l ~ list => list.foldLeft(l) {
      case (x, "*" ~ i) => Mult(x,i)
      case (x, "/" ~ i) => Div(x,i)
    }
  }

  // def typeName: Parser[String] = ( "int" | "float" | "double" | "chararray"| " bytearray") ^^ { s => s }

  def castTypeSpec: Parser[PigType] = (
    intKeyword ^^ { _ => Types.IntType }
      | longKeyword ^^ { _ => Types.LongType }
      | floatKeyword ^^ { _ => Types.FloatType }
      | doubleKeyword ^^ { _ => Types.DoubleType }
      | booleanKeyword ^^ { _ => Types.BooleanType }
      | chararrayKeyword ^^ { _ => Types.CharArrayType }
      | bytearrayKeyword ^^{ _ => Types.ByteArrayType }
      | tupleKeyword ~ "(" ~ repsep(castTypeSpec, ",") ~ ")" ^^{
            case _ ~ _ ~ typeList ~ _ => TupleType(typeList.map(t => Field("", t)).toArray)
        }
      /*
       * bag schema: bag{tuple(<list of types>)}
       */
     // | "bag" ~ "{" ~ "tuple" ~ "(" ~ ")" ~ repsep(castTypeSpec, ",") ~ "}" ^^{ case _ ~ "{" ~ tup ~ "}" => BagType("", tup) }
      /*
       * map schema: map[<list of types>]
       */
      | mapKeyword ~ "[" ~ "]" ^^{ case _ ~ _ ~ _ => MapType(Types.ByteArrayType) }
    )

  def factor: Parser[ArithmeticExpr] =  (
     "(" ~ castTypeSpec ~ ")" ~ refExpr ^^ { case _ ~ t ~ _ ~ e => CastExpr(t, e) }
      | "(" ~ arithmExpr ~ ")" ^^ { case _ ~ e ~ _ => PExpr(e) }
       | "flatten" ~ "(" ~ arithmExpr ~ ")" ^^ { case _ ~ _ ~ e ~ _  => FlattenExpr(e) }
       | typeConstructor
       | func
      | refExpr
    )

  def func: Parser[ArithmeticExpr] = className ~ "(" ~ repsep(arithmExpr, ",") ~ ")" ^^ { case f ~ _ ~ p ~ _ => Func(f, p) }
  def refExpr: Parser[ArithmeticExpr] = ref ^^ { r => RefExpr(r) }

  /*
   * And it can be also a type constrctor for tuple, bag or map.
  */
  def tupleConstructor: Parser[ArithmeticExpr] = "(" ~ repsep(arithmExpr, ",") ~ ")" ^^ { case _ ~ l ~ _ => ConstructTupleExpr(l) }
  def bagConstructor: Parser[ArithmeticExpr] = "{" ~ repsep(arithmExpr, ",") ~ "}"  ^^ { case _ ~ l ~ _ => ConstructBagExpr(l) }
  def mapConstructor: Parser[ArithmeticExpr] = "[" ~ repsep(arithmExpr, ",") ~ "]"  ^^ { case _ ~ l ~ _ => ConstructMapExpr(l) }
  def matrixConstructor: Parser[ArithmeticExpr] = matrixTypeName ~ "(" ~ num ~ "," ~ num ~ ", " ~ arithmExpr ~ ")" ^^{
    case s ~ _ ~ rows ~ _ ~ cols ~ _ ~ expr ~ _ => ConstructMatrixExpr(s.substring(0, 2).toLowerCase, rows, cols, expr)
  }



  def typeConstructor: Parser[ArithmeticExpr] = tupleConstructor | bagConstructor | mapConstructor | matrixConstructor | geometryConstructor

  def comparisonExpr: Parser[Predicate] = arithmExpr ~ ("!=" | "<=" | ">=" | "==" | "<" | ">") ~ (arithmExpr |
    pigStringLiteral ) ^^ {
    case a ~ op ~ (b: ArithmeticExpr) => op match {
      case "==" => Eq(a, b)
      case "!=" => Neq(a, b)
      case "<" => Lt(a, b)
      case "<=" => Leq(a, b)
      case ">" => Gt(a, b)
      case ">=" => Geq(a, b)
    }
    case a ~ op ~ (b: String) =>
      val b_val = RefExpr(Value(s""""${unquote(b)}""""))
      op match {
        case "==" => Eq(a, b_val)
        case "!=" => Neq(a, b_val)
        case "<" => Lt(a, b_val)
        case "<=" => Leq(a, b_val)
        case ">" => Gt(a, b_val)
        case ">=" => Geq(a, b_val)
      }
  }

  def boolLiteral = boolean ^^ { b => BoolLiteral(b)}

  def boolFactor: Parser[Predicate] = (
    boolLiteral
      | comparisonExpr ^^ { e => e }
      | "(" ~ boolExpr ~ ")" ^^ { case _ ~ e ~ _ => PPredicate(e) }
      | func ^^ { f => Eq(f,RefExpr(Value(true))) }
    )

  def boolNotFactor: Parser[Predicate] = opt(notKeyword) ~ boolFactor ^^ {
    case Some(_) ~ f => Not(f)
    case None ~ f => f
  }

  def boolTerm: Parser[Predicate] = boolNotFactor ~ rep(andKeyword ~ boolNotFactor) ^^{
    case f ~ list => list.foldLeft(f) {
        case (p1, _ ~ p2) => And(p1, p2)
      }
  }

  def boolExpr: Parser[Predicate] = boolTerm ~ rep(orKeyword ~ boolTerm) ^^{
    case f ~ list => list.foldLeft(f) {
      case (p1, _ ~ p2) => Or (p1, p2)
    }
  }

  /*
   * The list of case-insensitive keywords we want to accept.
   */
  lazy val intKeyword = "int".ignoreCase
  lazy val floatKeyword = "float".ignoreCase
  lazy val longKeyword = "long".ignoreCase
  lazy val doubleKeyword = "double".ignoreCase
  lazy val bytearrayKeyword = "bytearray".ignoreCase
  lazy val chararrayKeyword = "chararray".ignoreCase
  lazy val tupleKeyword = "tuple".ignoreCase
  lazy val mapKeyword = "map".ignoreCase
  lazy val bagKeyword = "bag".ignoreCase
  lazy val booleanKeyword = "boolean".ignoreCase

  lazy val loadKeyword = "load".ignoreCase
  lazy val dumpKeyword = "dump".ignoreCase
  lazy val displayKeyword = "display".ignoreCase
  lazy val storeKeyword = "store".ignoreCase
  lazy val intoKeyword = "into".ignoreCase
  lazy val filterKeyword = "filter".ignoreCase
  lazy val byKeyword = "by".ignoreCase
  lazy val groupKeyword = "group".ignoreCase
  lazy val cogroupKeyword = "cogroup".ignoreCase
  lazy val allKeyword = "all".ignoreCase
  lazy val joinKeyword = "join".ignoreCase
  lazy val crossKeyword = "cross".ignoreCase
  lazy val distinctKeyword = "distinct".ignoreCase
  lazy val defineKeyword = "define".ignoreCase
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
  lazy val socketReadKeyword = "socket_read".ignoreCase
  lazy val socketWriteKeyword = "socket_write".ignoreCase
  lazy val modeKeyword = "mode".ignoreCase
  lazy val zmqKeyword = "zmq".ignoreCase
  lazy val windowKeyword = "window".ignoreCase
  lazy val rowsKeyword = "rows".ignoreCase
  lazy val rangeKeyword = "range".ignoreCase
  lazy val slideKeyword = "slide".ignoreCase
  lazy val splitKeyword = "split".ignoreCase
  lazy val ifKeyword = "if".ignoreCase
  lazy val materializeKeyword = "materialize".ignoreCase
  lazy val cacheKeyword = "cache".ignoreCase
  lazy val rscriptKeyword = "rscript".ignoreCase
  lazy val rdfLoadKeyword = "rdfload".ignoreCase
  lazy val groupedOnKeyword = "grouped on".ignoreCase
  lazy val trueKeyword = "true".ignoreCase
  lazy val falseKeyword = "false".ignoreCase
  lazy val fsKeyword = "fs".ignoreCase
  lazy val setKeyword = "set".ignoreCase
  lazy val returnsKeyword = "returns".ignoreCase
  lazy val accumulateKeyword = "accumulate".ignoreCase
  lazy val timestampKeyword = "timestamp".ignoreCase
  lazy val zipKeyword = "zip".ignoreCase

  def boolean: Parser[Boolean] = (
      trueKeyword ^^ { _=> true }
      | falseKeyword ^^ { _ => false }
    )


  /*
   * tuple schema: tuple(<list of fields>) or (<list of fields>)
   */
  def tupleTypeSpec: Parser[TupleType] =
    (tupleKeyword ?) ~ "(" ~repsep(fieldSchema, ",") ~ ")" ^^{ case _ ~ _ ~ fieldList ~ _ => TupleType(fieldList.toArray) }

  def matrixTypeName: Parser[String] = "[sdSD][idID][mM][aA][tT][rR][iI][xX]".r
  def matrixTypeSpec: Parser[PigType] = matrixTypeName ~ "(" ~ num ~ "," ~ num ~ ")" ^^{
    case n ~ _ ~ rows ~ _ ~ cols ~ _ =>
      val t = if (n.charAt(1) == 'i' || n.charAt(1) == 'I') Types.IntType else Types.DoubleType
      val rep = if (n.charAt(0) == 's' || n.charAt(0) == 'S') MatrixRep.SparseMatrix else MatrixRep.DenseMatrix
      MatrixType(t, rows, cols, rep)
  }

//  def geometryTypeSpec: Parser[GeometryType] = geometryTypeName ~ "(" ~ stringLiteral ~ ")" ^^ {
//    case _ ~ _ ~ wkt ~ _ =>  GeometryType(wkt)
//
//  }

  def typeSpec: Parser[PigType] = (
    intKeyword ^^ { _ => Types.IntType }
    | longKeyword ^^ { _ => Types.LongType }
    | floatKeyword ^^ { _ => Types.FloatType }
    | doubleKeyword ^^ { _ => Types.DoubleType }
    | booleanKeyword ^^ { _ => Types.BooleanType }
    | chararrayKeyword ^^ { _ => Types.CharArrayType }
    | bytearrayKeyword ^^{ _ => Types.ByteArrayType }
    | tupleTypeSpec
      /*
       * bag schema: bag{<tuple>} or {<tuple>}
       */
    | (bagKeyword ?) ~ "{" ~ tupleTypeSpec ~ "}" ^^{ case _ ~  _ ~ tup ~ _ => BagType(tup) }
      /*
       * map schema: map[<list of fields>] or [<list of fields>]
       */
    | (mapKeyword ?) ~ "[" ~(typeSpec?) ~ "]" ^^{ case _ ~ _ ~ ty ~ _ => ty match {
        case Some(t) => MapType(t)
        case None => MapType(Types.ByteArrayType)
    }}
    | matrixTypeSpec
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
   * <A> = LOAD <B> "<FileName>" USING <StorageFunc> (<OptParameters>) [ AS (<Schema>) ] [ TIMESTAMP(<field>) ]
   */
  def loadSchemaClause: Parser[Schema] = asKeyword ~ "(" ~ repsep(fieldSchema, ",") ~ ")" ^^{
    case _ ~ _ ~ fieldList ~ _ => Schema(BagType(TupleType(fieldList.toArray)))
  }

  def usingClause: Parser[(String, List[String])] = usingKeyword ~ ident ~ "(" ~ repsep(params, ",") ~ ")" ^^ {
    case _ ~ loader ~ _ ~ params ~ _ => (loader, params)
  }

  def params: Parser[String] = kvParam | plainParams

  def kvParam = ident ~ "=" ~ plainParams ^^ {
    case k ~ _ ~ v =>
      val v2 = if(v.startsWith("'") && v.endsWith("'"))
                 s""""${unquote(v)}""""
               else
                 v

      s"$k=$v2"
  }

  def plainParams = (boolLiteral | decimalNumber | num | pigStringLiteral) ^^ {
    case p: BoolLiteral => p.b.toString
    case p => p.toString
  }

  def fieldRef: Parser[Ref] = posField | namedFieldWithoutLineage
  def timestampClause: Parser[Ref] = timestampKeyword ~ "(" ~ fieldRef ~ ")" ^^ { case _ ~ _ ~ f ~ _ => f }

  def loadStmt: Parser[PigOperator] = bag ~ "=" ~ loadKeyword ~ fileName ~ (usingClause?) ~ (loadSchemaClause?) ~ (timestampClause?) ^^ {
    case b ~ _ ~ _ ~ f ~ u ~ s ~ ts =>
      val uri = f //new URI(f)
      if (s.isDefined && ts.isDefined) ts.get match {
        case NamedField(n, _) => s.get.timestampField = s.get.indexOfField(n)
        case PositionalField(p) => s.get.timestampField = p
        case _ =>
      }

      u match {
        case Some(p) =>
          val params = if (p._2.isEmpty) null // no params given
            else {
              // transform given params to convert ' into " - if it's an unquoted param (e.g. int, boolean values) leave it as is
              p._2.map(s =>
                if(s.startsWith("'") && s.endsWith("'"))
                  s""""${unquote(s)}""""
                else
                  s
              )
            }

          val s2 = if(p._1.toLowerCase() == "textloader" && s.isEmpty) {
              /* If no schema is given for text loader, create one implicitly.
               * The schema will be one field with name "line" and type chararray
               */
              Some(Schema(BagType(TupleType(Array(Field("line", Types.CharArrayType))))))
            }
            else
              s

          Load(Pipe(b), uri, s2, Some(p._1), params)

        case None => Load(Pipe(b), uri, s)
      }
  }

  def groupedOnClause: Parser[String] = groupedOnKeyword ~ ("subject" | "predicate" | "object") ^^ {
    case _ ~ groupingColumn => groupingColumn
  }

  /*
   * <A> = RDFLOAD('<FileName>') grouped on <subject|predicate|object>;
   */

  def rdfLoadStmt: Parser[PigOperator] = bag ~ "=" ~ rdfLoadKeyword ~ "(" ~ fileName ~ ")" ~ (groupedOnClause?) ^^ {
    case b ~ _ ~ _ ~ _ ~ filename ~ _ ~ grouped =>
      val uri = new URI(filename)
      new RDFLoad(Pipe(b), uri, grouped)
  }

  lazy val mute = "mute".ignoreCase

  /*
   * DUMP <A>
   */
  def dumpStmt: Parser[PigOperator] = dumpKeyword ~ bag ~ (mute?) ^^ { case _ ~ b ~ nuller => Dump(Pipe(b), nuller.isDefined) }

  /*
   * DISPLAY <A>
   */
  def displayStmt: Parser[PigOperator] = displayKeyword ~ bag ^^ { case _ ~ b => Display(Pipe(b)) }

  /*
   * STORE <A> INTO "<FileName>"
   */
  def storeStmt: Parser[PigOperator] = storeKeyword ~ bag ~ intoKeyword ~ fileName ~ (usingClause?) ^^ {
    case _ ~ b ~  _ ~ f ~ u =>
      val uri = f //new URI(f)
      u match {
        case Some(p) => Store(Pipe(b), uri, Some(p._1), if (p._2.isEmpty) null else p._2.map(s => s""""${unquote(s)}""""))
        case None => Store(Pipe(b), uri)
      }
  }

  /*
   * GENERATE expr1, expr2, ...
   */
  def generateStmt: Parser[PigOperator] = plainForeachGenerator ^^ (g => Generate(g.asInstanceOf[GeneratorList].exprs))

  /*
   * B = A.C;
   */
  def constructBagStmt: Parser[PigOperator] = bag ~ "=" ~ derefBagOrTuple ^^ { case res ~ _ ~ ref => ConstructBag(Pipe(res), ref) }

  /*
   * Currently, Pig allows only DISTINCT, LIMIT, FILTER, ORDER BY + GENERATE and assignment inside FOREACH
   */
  def nestedStmt: Parser[PigOperator] = (distinctStmt | limitStmt | filterStmt | orderByStmt |
    generateStmt | constructBagStmt) ~ ";" ^^ { case op ~ _  => op }
  def nestedScript: Parser[List[PigOperator]] = rep(nestedStmt)

  /*
   * <A> = FOREACH <B> GENERATE <Expr> [ AS <Schema> ]
   * <A> = FOREACH <B> { <SubPlan> }
   */
  def exprSchema: Parser[Field] = asKeyword ~ fieldSchema ^^ { case _ ~ t => t }
  def simpleGeneratorExpr: Parser[GeneratorExpr] = arithmExpr ~ (exprSchema?) ^^ { case e ~ s => GeneratorExpr(e, s) }
  def asteriskExpr: Parser[GeneratorExpr] = "*" ^^ (_ => GeneratorExpr(RefExpr(NamedField("*"))))
  def genExpr: Parser[GeneratorExpr] = asteriskExpr | simpleGeneratorExpr
  def generatorList: Parser[List[GeneratorExpr]] = repsep(genExpr, ",")
  def plainForeachGenerator: Parser[ForeachGenerator] = generateKeyword ~ generatorList ^^ {
    case _ ~ exList => GeneratorList(exList)
  }
  def nestedForeachGenerator: Parser[ForeachGenerator] = "{" ~ nestedScript ~ "}" ^^ {
    case _ ~ opList ~ _ => GeneratorPlan(opList)
  }
  def foreachStmt: Parser[PigOperator] = bag ~ "=" ~ foreachKeyword ~ bag ~
    (plainForeachGenerator | nestedForeachGenerator) ^^ {
      case out ~ _ ~ _ ~ in ~ ex => Foreach(Pipe(out), Pipe(in), ex)
    }

  /*
   * <A> = ACCUMULATE <B> GENERATE <Expr> [ AS <Schema> ]
   */
  def accumulateStmt: Parser[PigOperator] = bag ~ "=" ~ accumulateKeyword ~ bag ~ generateKeyword ~ generatorList ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ exList => Accumulate(Pipe(out), Pipe(in), GeneratorList(exList))
  }

  /*
   * <A> = FILTER <B> BY <Predicate>
   */
  def filterStmt: Parser[PigOperator] = bag ~ "=" ~ filterKeyword ~ bag ~ byKeyword ~ boolExpr ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ pred => Filter(Pipe(out), Pipe(in), pred)
  }

  /*
   * DESCRIBE <A>
   */
  def describeStmt: Parser[PigOperator] = describeKeyword ~ bag ^^ { case _ ~ b => Describe(Pipe(b)) }


  def cogroupList = bag ~ groupingClause ^^ {
    case in ~ expr => (in, expr)
  }

  def cogroupStmt = bag ~ "=" ~ cogroupKeyword ~ repsep(cogroupList , ",") ^^ {
    case out ~ _ ~ _ ~ exprs =>
      val inputs = exprs.map(t => Pipe(t._1))
      val refs = exprs.map(t => t._2)

      CoGroup(Pipe(out), inputs, refs)
  }

  /*
   * <A> = GROUP <B> ALL
   * <A> = GROUP <B> BY <Ref>
   * <A> = GROUP <B> B> ( <ListOfRefs> )
   */
  def refList: Parser[List[Ref]] = ref ^^ { r => List(r) } | "(" ~ repsep(ref, ",") ~ ")" ^^ { case _ ~ rlist ~ _ => rlist }

  def groupingClause: Parser[GroupingExpression] = allKeyword ^^ {
    _ => GroupingExpression(List())} |
    (byKeyword ~ refList ^^ {
      case _ ~ rlist => GroupingExpression(rlist)
    })

  def groupingStmt: Parser[PigOperator] = bag ~ "=" ~ groupKeyword ~ bag ~ groupingClause ^^ {
    case out ~ _ ~ _ ~ in ~ grouping => Grouping(Pipe(out), Pipe(in), grouping) }

  /*
   * <A> = DISTINCT <B>
   */
  def distinctStmt: Parser[PigOperator] = bag ~ "=" ~ distinctKeyword ~ bag ^^ { case out ~ _ ~ _ ~ in => Distinct(Pipe(out), Pipe(in), windowMode = false) }

  /*
   * <A> = LIMIT <B> <Num>
   */
  def limitStmt: Parser[PigOperator] = bag ~ "=" ~ limitKeyword ~ bag ~ num ^^ { case out ~ _ ~ _ ~ in ~ num => Limit(Pipe(out), Pipe(in), num) }

  /*
   * <A> = JOIN <B> BY <Ref>, <C> BY <Ref>, ...
   * <A> = JOIN <B> BY ( <ListOfRefs> ), <C> BY ( <ListOfRefs>), ...
   */
  def joinExpr: Parser[(String, List[Ref])] = bag ~ byKeyword ~ refList ^^ { case b ~ _ ~ rlist => (b, rlist) }
  def joinExprList: Parser[List[(String, List[Ref])]] = repsep(joinExpr, ",") ^^ (jlist => jlist)
  def extractJoinRelation(jList: List[(String, List[Ref])]): List[Pipe] = { jList.map{ case (alias, _) => Pipe(alias) } }
  def extractJoinFields(jList: List[(String, List[Ref])]): List[List[Ref]] = { jList.map{ case (_, refs) => refs } }
  def joinStmt: Parser[PigOperator] = bag ~ "=" ~ joinKeyword ~ joinExprList ^^ {
    case out ~ _ ~ _ ~ jlist => Join(Pipe(out), extractJoinRelation(jlist), extractJoinFields(jlist)) }

  /*
   * <A> = CROSS <B>, <C>, ...
   */
  def crossStmt: Parser[PigOperator] = bag ~ "=" ~ crossKeyword ~ repsep(bag, ",") ^^ {
    case out ~ _ ~ _ ~ rlist => Cross(Pipe(out), rlist.map(r => Pipe(r)))
  }


  def zipExpr = withKeyword ~ bag ^^ { case _ ~ b => b}
  def zipExprList = rep(zipExpr) ^^ { l => l}

  def zipWithIndexStmt = bag ~ "=" ~ zipKeyword ~ bag ~ withKeyword ~ indexKeyword ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ _ => Zip(Pipe(out), List(Pipe(in)), withIndex = true)
  }

  def zipWithBagsStmt = bag ~ "=" ~ zipKeyword ~ bag ~ zipExprList ^^ {
    case out ~ _ ~ _ ~ in ~ bags => Zip(Pipe(out), List(Pipe(in)) ++ bags.map(b => Pipe(b)), withIndex = false)
  }

  def zipStmt = zipWithIndexStmt | zipWithBagsStmt


  /*
   * <A> = UNION <B>, <C>, <D>, ...
   */
  def unionStmt: Parser[PigOperator] = bag ~ "=" ~ unionKeyword ~ repsep(bag, ",") ^^ {
    case out ~ _ ~ _ ~ rlist => Union(Pipe(out), rlist.map(r => Pipe(r)))
  }

  /*
   * REGISTER <JarFile>
   */
  def registerStmt: Parser[PigOperator] = registerKeyword ~ fileName ^^{ case _ ~ uri => RegisterCmd(uri) }

  /*
   * DEFINE <Alias> <FuncName>
   */
  def defineStmt: Parser[PigOperator] = defineKeyword ~ ident ~ className ~ "(" ~ repsep(literalField, ",") ~ ")" ^^{
    case _ ~ alias ~ funcName ~ _ ~ params ~ _ => DefineCmd(alias, funcName, params.map(r => r.asInstanceOf[dbis.piglet.expr.Value]))
  }

  /*
   * DEFINE <Macro> ( [ParamList] ) RETURNS <Alias> { <Block> }
   */
  def paramNameList: Parser[List[String]] = "(" ~ repsep(ident, ",") ~ ")" ^^ { case _ ~ plist ~ _ => plist}

  def defineMacroStmt: Parser[PigOperator] = defineKeyword ~ ident ~ (paramNameList?) ~ returnsKeyword ~ bag ~ "{" ~ rep(pigStmt) ~ "}" ~ ";" ^^{
    case _ ~ macroName ~ params ~ _ ~ out ~ _ ~ stmtList ~ _  ~ _ => DefineMacroCmd(Pipe(out), macroName, params, stmtList)
  }

  def paramList: Parser[List[Ref]] = "(" ~ repsep(ref, ",") ~ ")" ^^ { case _ ~ rlist ~ _ => rlist}

  /*
   * <A> = <MacroName> ( [<ParamList>] )
   */
  def macroRefStmt: Parser[PigOperator] = bag ~ "=" ~ ident ~ (paramList?) ^^ {
    case out ~ _ ~ macroName ~ params => MacroOp(Pipe(out), macroName, params)
  }

  /*
   * SET <Param> <Value>
   */
  def setterStmt: Parser[PigOperator] = setKeyword ~ ident ~ literalField ^^ {
    case _ ~ k ~ v => SetCmd(k, v.asInstanceOf[dbis.piglet.expr.Value]) }

  /*
   * <A> = STREAM <B> TROUGH <Operator> [(ParamList)] [AS (<Schema>) ]
   */
  def streamStmt: Parser[PigOperator] = bag ~ "=" ~ streamKeyword ~ bag ~ throughKeyword ~ className ~ (paramList?) ~ (loadSchemaClause?) ^^{
    case out ~ _ ~_ ~ in ~ _ ~ opname ~ params ~ schema => StreamOp(Pipe(out), Pipe(in), opname, params, schema)
  }

  /*
   * <A> = SAMPLE <B> <num>
   * <A> = SAMPLE <B> <Expr>
   */
  def sampleStmt: Parser[PigOperator] = bag ~ "=" ~ sampleKeyword ~ bag ~ arithmExpr ^^ {
    case out ~ _ ~ _ ~ in ~ expr => Sample(Pipe(out), Pipe(in), expr)
  }

  /*
   * * [ASC | DESC]
   * field [ASC | DESC], field [ASC | DESC]
   */
  import OrderByDirection._

  def sortOrder: Parser[OrderByDirection] = ascKeyword ^^ { _ => OrderByDirection.AscendingOrder } |
    descKeyword ^^ { _ => OrderByDirection.DescendingOrder }

  def allOrderSpec: Parser[OrderBySpec] = "*" ~ (sortOrder?) ^^ { case _ ~ o => o match {
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

  def orderSpec: Parser[List[OrderBySpec]] = allOrderSpec ^^ { s => List(s) } | repsep(fieldOrderSpec, ",")

  /*
   * <A> = ORDER <B> BY <OrderSpec>
   */
  def orderByStmt: Parser[PigOperator] = bag ~ "=" ~ orderKeyword ~ bag ~ byKeyword ~ orderSpec ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ spec => OrderBy(Pipe(out), Pipe(in), spec)
  }

  /*
   * SPLIT <A> INTO <B> IF <Cond>, <C> IF <Cond> ...
   */
  def splitBranch: Parser[SplitBranch] = bag ~ ifKeyword ~ boolExpr ^^ { case out ~ _ ~ expr => SplitBranch(Pipe(out), expr)}

  def splitStmt: Parser[PigOperator] = splitKeyword ~ bag ~ intoKeyword ~ repsep(splitBranch, ",") ^^ {
    case _ ~ in ~ _ ~ splitList => SplitInto(Pipe(in), splitList)
  }

  /*
   * MATERIALIZE <A>
   */
  def materializeStmt: Parser[PigOperator] = materializeKeyword ~ bag ^^ { case _ ~ b => Materialize(Pipe(b))}


  def cacheModeStmt = ident ^^ { mode => CacheMode.withName(mode.toUpperCase) }
  /*
   * <A> = CACHE <B> <MODE>
   */
  def cacheStmt = bag ~ "=" ~ cacheKeyword ~ bag ~ (cacheModeStmt?) ^^ {
    case out ~ _ ~ _ ~ in ~ mode =>
      val theMode = mode match {
        case Some(m) => m
        case None => CacheMode.NONE
      }

      Cache(Pipe(out), Pipe(in), in, theMode) //operatorId is set to "in" - it's just for distinguishing in equals
  }

  def rscriptStmt: Parser[PigOperator] = bag ~ "=" ~ rscriptKeyword ~ bag ~ usingKeyword ~ pigStringLiteral ~ (loadSchemaClause?) ^^{
    case out ~ _ ~ _ ~ in ~ _ ~ script ~ schema => RScript(Pipe(out), Pipe(in), script, schema)
  }

  /*
   * fs -cmd params
   */
  def fsCmd: Parser[HdfsCommand.HdfsCommand] = """-[a-zA-Z]*""".r ^^ { s =>
    try {
      HdfsCommand.withName(s.substring(1).toUpperCase())
    } catch {
      case _: NoSuchElementException => throw new IllegalArgumentException(s"No such HDFS command: $s")
    }

  }
  def fsParam: Parser[String] = """[^;][/\w\.\-]*""".r

  def fsStmt: Parser[PigOperator] = fsKeyword ~ fsCmd ~ rep(fsParam) ^^ {
    case _ ~ cmd ~ params => HdfsCmd(cmd, params)
  }

  def code = """(?s)(.*?)"""

  def embeddedCode: Parser[EmbedCmd] = "<%" ~ (code + "%>").r ^^ {
    case _ ~ code if code.split("rules:").length != 2 => new EmbedCmd(code.substring(0, code.length - 2))
    case _ ~ code =>
      val ctuple = code.substring(0, code.length -2 ).split("rules:")
      EmbedCmd(ctuple(0), Some(ctuple(1)))
  }

  def embedStmt: Parser[PigOperator] = embeddedCode

  /*
   * A statement can be one of the above delimited by a semicolon.
   */
  def delimStmt: Parser[PigOperator] =
    /* streaming statements */
    socketReadStmt | socketWriteStmt | windowStmt |
    /* SPARQL statements */
    tuplifyStmt | bgpFilterStmt | rdfLoadStmt |
    /* CEP statement */
    matcherStmt |
    /* spatial statements */
    spatialFilterStmt | spatialJoinStmt | /*indexStmt |*/ partitionStmt |
    /* misc Pig Latin extensions */
    materializeStmt | cacheStmt | delayStmt | rscriptStmt |
    /* standard Pig Latin statements */
    loadStmt | dumpStmt | describeStmt | foreachStmt | filterStmt | groupingStmt | cogroupStmt | accumulateStmt |
    distinctStmt |  joinStmt | crossStmt | zipStmt | storeStmt | limitStmt | unionStmt | registerStmt | streamStmt | sampleStmt | orderByStmt |
    splitStmt | fsStmt | defineStmt | setterStmt | macroRefStmt | displayStmt





  /* ---------------------------------------------------------------------------------------------------------------- */
  /*
   * Pig extensions for processing RDF data and supporting SPARQL BGPs.
   */
  lazy val tuplifyKeyword = "tuplify".ignoreCase
  lazy val onKeyword = "on".ignoreCase
  lazy val bgpFilterKeyword = "bgp_filter".ignoreCase

  def tuplifyStmt: Parser[PigOperator] = bag ~ "=" ~ tuplifyKeyword ~ bag ~ onKeyword ~ ref ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ r => Tuplify(Pipe(out), Pipe(in), r) }

  def bgpVariable: Parser[NamedField] = "?" ~ ident ^^ { case _ ~ varname => NamedField(varname)}

  def bgPattern: Parser[TriplePattern] = (bgpVariable | ref) ~(bgpVariable | ref) ~ (bgpVariable | ref) ^^ {
    case r1 ~ r2 ~ r3 => TriplePattern(r1, r2, r3)
  }

  def bgpFilterStmt: Parser[PigOperator] = bag ~ "=" ~ bgpFilterKeyword ~ bag ~
    byKeyword ~ "{" ~ repsep(bgPattern, ".") ~ "}" ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ _ ~ pattern ~ _ => BGPFilter(Pipe(out), Pipe(in), pattern)
  }

  /* ---------------------------------------------------------------------------------------------------------------- */
  /*
   * Pig extensions for processing streaming data.
   */

  /*
   * <A> = WINDOW <B> ROWS  <Num> SLIDE ROWS <Num>
   * <A> = WINDOW <B> ROWS  <Num> SLIDE RANGE <Num> <Unit>
   * <A> = WINDOW <B> RANGE <Num> <Unit> SLIDE ROWS <Num>
   * <A> = WINDOW <B> RANGE <Num> <Unit> SLIDE RANGE <Num> <Unit>
   */
  def timeUnit: Parser[String] = "seconds".ignoreCase | "minutes".ignoreCase //^^ { _.toUpperCase }
  def rangeParam: Parser[(Int, String)] = rangeKeyword ~ num ~ timeUnit ^^ {case _ ~ n ~ u => (n,u)}
  def rowsParam: Parser[(Int, String)] = rowsKeyword ~ num ^^ {case _ ~ n => (n, "")}
  def windowParam: Parser[(Int, String)] = rangeParam | rowsParam
  def windowStmt: Parser[PigOperator] = bag ~ "=" ~ windowKeyword ~ bag ~ windowParam ~ slideKeyword ~ windowParam ^^ {
    case out ~ _ ~ _ ~ in ~ on ~ _ ~ slide => Window(Pipe(out), Pipe(in), on, slide)
  } | bag ~ "=" ~ windowKeyword ~ bag ~ windowParam ^^ {
    case out ~ _ ~ _ ~ in ~ on => Window(Pipe(out), Pipe(in), on, on)
  }

  /*
   * Socket Definitions
   */
  def ipMember: Parser[String] = "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)".r
  def ipv4: Parser[String] = ipMember ~ "." ~ ipMember ~ "." ~ ipMember ~ "." ~ ipMember ^^{
    case i1 ~ _ ~ i2 ~ _ ~ i3 ~ _ ~i4 => i1 + "." + i2 + "." + i3 + "." + i4
  }
  def portNum: Parser[String] = "([0-9]{1,5})".r
  def port: Parser[String] = portNum | "*"
  def bindAddress: Parser[String] = ipv4 | "*" | ident

  def inetAddress: Parser[SocketAddress] = "'" ~ (ipv4 | ident) ~ ":" ~ portNum ~ "'" ^^ { case _ ~ ip ~ _ ~ p ~ _ => SocketAddress("",ip,p)}
  def tcpSocket: Parser[SocketAddress] = "tcp://" ~ bindAddress ~ ":" ~ port ^^ { case trans ~ addr ~ _ ~ p => SocketAddress(trans, addr, p)}
  def ipcSocket: Parser[SocketAddress] = "ipc://" ~ (fileName | "*") ^^ { case trans ~  path => SocketAddress(trans,path,"")}
  def inprocSocket: Parser[SocketAddress] = "inproc://" ~ ident ^^ { case trans ~ name => SocketAddress(trans,name,"")}
  def pgmSocket: Parser[SocketAddress] = ("pgm://" | "epgm://") ~ (ipv4 | ident) ~ ";" ~ ipv4 ~ ":" ~ portNum ^^ {
    case trans ~ interface ~ _ ~ ip ~ _ ~ p => SocketAddress(trans, interface + ";" + ip, p)
  }
  def zmqAddress: Parser[SocketAddress] = "'" ~ (tcpSocket | ipcSocket | inprocSocket | pgmSocket) ~ "'" ^^ { case _ ~ addr ~ _ => addr}

  /*
   * <A> = SOCKET_READ '<address>' [ MODE ZMQ ] USING <StreamFunc> [ AS <schema> ] [ TIMESTAMP (<field>) ]
   *
   * Maybe other modes later
   */
  def socketReadStmt: Parser[PigOperator] =
    bag ~ "=" ~ socketReadKeyword ~ inetAddress ~ (usingClause?) ~ (loadSchemaClause?) ~ (timestampClause?) ^^ {
      case out ~ _ ~ _ ~ addr ~ u ~ schema ~ ts =>
        if (schema.isDefined && ts.isDefined) ts.get match {
          case NamedField(n, _) => schema.get.timestampField = schema.get.indexOfField(n)
          case PositionalField(p) => schema.get.timestampField = p
          case _ =>
        }
        u match {
          case Some(p) => SocketRead(Pipe(out), addr, "", schema, Some(p._1), if (p._2.isEmpty) null else p._2.map(s => s""""${unquote(s)}""""))
          case None =>  SocketRead(Pipe(out), addr, "", schema)
      }

    } |
      bag ~ "=" ~ socketReadKeyword ~ zmqAddress ~ modeKeyword ~ zmqKeyword ~ (usingClause?) ~ (loadSchemaClause?) ~ (timestampClause?) ^^ {
        case out ~ _ ~ _ ~ addr ~ _ ~ mode ~ u ~ schema ~ ts =>
          if (schema.isDefined && ts.isDefined) ts.get match {
            case NamedField(n, _) => schema.get.timestampField = schema.get.indexOfField(n)
            case PositionalField(p) => schema.get.timestampField = p
            case _ =>
          }
          u match {
          case Some(p) => SocketRead(Pipe(out), addr, mode, schema, Some(p._1), if (p._2.isEmpty) null else p._2.map(s => s""""${unquote(s)}""""))
          case None => SocketRead(Pipe(out), addr, mode, schema)
        }
      }

  /*
   * SOCKET_WRITE <A> TO '<address>' [ MODE ZMQ ]
   */
  def socketWriteStmt: Parser[PigOperator] =
    socketWriteKeyword ~ bag ~ toKeyword ~ inetAddress ~ (usingClause?) ^^ {
      case _ ~ b ~ _ ~ addr ~ u => u match {
        case Some(p) => SocketWrite(Pipe(b), addr, "", Some(p._1), if (p._2.isEmpty) null else p._2.map(s => s""""${unquote(s)}""""))
        case None =>  SocketWrite(Pipe(b), addr)
      }
    } |
      socketWriteKeyword ~ bag ~ toKeyword ~ zmqAddress ~ modeKeyword ~ zmqKeyword ~ (usingClause?) ^^ {
        case _ ~ b ~ _ ~ addr ~ _ ~ mode ~ u => u match {
          case Some(p) => SocketWrite(Pipe(b), addr, mode, Some(p._1), if (p._2.isEmpty) null else p._2.map(s => s""""${unquote(s)}""""))
          case None =>  SocketWrite(Pipe(b), addr, mode)
        }
      }

  /* ---------------------------------------------------------------------------------------------------------------- */

  /* ------------------------------------------------------------ */
  /*        Pig extensions for complex event processing.
   * ------------------------------------------------------------
   */
  lazy val skipNext = "skip_till_next_match"
  lazy val matchEventKeyword = "match_event".ignoreCase
  lazy val patternKeyword = "pattern".ignoreCase
  lazy val withKeyword = "with".ignoreCase
  lazy val withinKeyword = "within".ignoreCase
  lazy val negKeyword = "neg".ignoreCase
  lazy val conjKeyword = "and".ignoreCase
  lazy val disjKeyword = "or".ignoreCase
  lazy val seqKeyword = "seq".ignoreCase
  lazy val skipNextKeyword = skipNext.ignoreCase
  lazy val skipAnyKeyword = "skip_till_any_match".ignoreCase
  lazy val firstMatchKeyword = "first_match".ignoreCase
  lazy val recentMatchKeyword = "recent_match".ignoreCase
  lazy val cognitiveMatchKeyword = "cognitive_match".ignoreCase

  def eventExpr: Parser[Predicate] = boolExpr
  def simpleEvent: Parser[SimpleEvent] = simplePattern ~ ":" ~ eventExpr ^^ { case s ~ _ ~ e => SimpleEvent(s, e) }
  def eventParam: Parser[CompEvent] = "(" ~ simpleEvent ~ rep( "," ~> simpleEvent) ~ ")" ^^ { case _ ~ s ~ c ~ _ => CompEvent(s :: c) }

  def repeatPattern: Parser[List[Pattern]] = rep("," ~> patternParam)
  // or  def repeatPattern: Parser[List[Pattern]] = rep(","  ~ patternParam)  ^^ { case l => l.map(_._2) }
  def disjPattern: Parser[Pattern] = disjKeyword ~ "(" ~ patternParam ~ "," ~ patternParam ~ repeatPattern ~ ")" ^^ {
    case _ ~ _ ~ d1 ~ _ ~ d2 ~ d ~ _ => DisjPattern(d1 :: d2 :: d)
  }
  def conjPattern: Parser[Pattern] = conjKeyword ~ "(" ~ patternParam ~ "," ~ patternParam ~ repeatPattern ~ ")" ^^ {
    case _ ~ _ ~ c1 ~ _ ~ c2 ~ c ~ _ => ConjPattern(c1 :: c2 :: c)
  }
  def seqPattern: Parser[Pattern] = seqKeyword ~ "(" ~ patternParam ~ "," ~ patternParam ~ repeatPattern ~ ")" ^^ {
    case _ ~ _ ~ s1 ~ _ ~ s2 ~ s ~ _ => SeqPattern(s1 :: s2 :: s)
  }
  def negPattern: Parser[Pattern] = negKeyword ~ "(" ~ simplePattern ~ ")" ^^ { case _ ~ _ ~ n ~ _ => NegPattern(n) }
  def simplePattern: Parser[Pattern] = ident ^^ (s => SimplePattern(s))
  def patternParam: Parser[Pattern] = seqPattern | negPattern | conjPattern | disjPattern | simplePattern

  def withinParam: Parser[(Int, String)] = withinKeyword ~ num ~ timeUnit ^^ { case _ ~ n ~ u => (n, u) }
  def modes: Parser[String] = skipNextKeyword | skipAnyKeyword | firstMatchKeyword | recentMatchKeyword | cognitiveMatchKeyword
  def modeParam: Parser[String] = modeKeyword ~ modes ^^ { case _ ~ n => n }
  def matcherStmt: Parser[PigOperator] = bag ~ "=" ~ matchEventKeyword ~ bag ~ patternKeyword ~ patternParam ~ withKeyword ~ eventParam ~ (modeParam?) ~ (withinParam?) ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ pattern ~ _ ~ e ~ mode ~ within => mode match {
      case Some(m) => within match {
        case Some(w) => Matcher(Pipe(out), Pipe(in), pattern, e, m, w)
        case None    => Matcher(Pipe(out), Pipe(in), pattern, e, m)
      }
      case None => within match {
        case Some(w) => Matcher(Pipe(out), Pipe(in), pattern, e, skipNext, w)
        case None    => Matcher(Pipe(out), Pipe(in), pattern, e)
      }
    }
  }

  /* ---------------------------------------------------------------------------------------------------------------- */



  /* ------------------------------------------------------------ */
  /*        Pig extensions for spatial data
   * ------------------------------------------------------------
   */

  lazy val geometryTypeName = "geometry".ignoreCase
  lazy val spatialJoinKeyword = "spatial_join".ignoreCase
  lazy val spatialFilterKeyword = "spatial_filter".ignoreCase
  lazy val containsKeyword = "contains".ignoreCase
  lazy val containedByKeyword = "containedBy".ignoreCase
  lazy val intersectsKeyword = "intersects".ignoreCase
  lazy val indexKeyword = "index".ignoreCase
  lazy val rtreeKeyword = "rtree".ignoreCase


  def instant = arithmExpr  ^^ (e => Instant(e))

  def interval = arithmExpr ~ "," ~ arithmExpr ^^ { case s ~ _ ~ e => Interval(s,Some(e)) }

                            // the comma has to be here because it's also optional
  def timeExp: Parser[TempEx] = "," ~ (interval | instant) ^^ { case _ ~ time => time }

  def geometryConstructor = geometryTypeName ~ "(" ~ arithmExpr ~ (timeExp?) ~ ")" ^^ {
    case _ ~ _ ~ geo ~ time ~ _ => ConstructGeometryExpr(geo, time)
  }

  def withIndexClause = usingKeyword ~ indexKeyword ~ indexMethod ^^ {
    case _ ~ _ ~ method => method
  }
  
  def joinRefs = "(" ~ ref ~ "," ~ ref ~ ")" ^^ {
    case _ ~ r1 ~ _ ~ r2 ~ _ => (r1,r2)
  }
  
  def spatialJoinPredicate = (containsKeyword | intersectsKeyword | containedByKeyword) ~ (joinRefs?) ^^ {
    case kw ~ refs =>

      val (r1,r2) = refs match {
        case None => (PositionalField(0),PositionalField(0))
        case Some(r) => r
      }
      SpatialJoinPredicate(r1, r2, SpatialPredicateType.withName(kw.toUpperCase()))
  }

  def spatialJoinStmt: Parser[PigOperator] = bag ~ "=" ~ spatialJoinKeyword ~ bag ~ "," ~ bag ~ onKeyword ~ spatialJoinPredicate ~ (withIndexClause?)  ^^ {
    case out ~ _ ~ _ ~ in1 ~ _ ~ in2  ~ _ ~ expr ~ idx => SpatialJoin(Pipe(out), List(Pipe(in1), Pipe(in2)), expr, idx)

  }

  def spatialFilterPredicate = (containsKeyword | intersectsKeyword | containedByKeyword) ~ "(" ~ ref ~ "," ~ geometryConstructor ~ ")" ^^ {
    case kw ~ _ ~ f ~ _ ~ expr ~ _ => SpatialFilterPredicate(f, expr, SpatialPredicateType.withName(kw.toUpperCase()))
  }

  
  
  def spatialFilterStmt = bag ~ "=" ~ spatialFilterKeyword ~ bag ~ byKeyword ~ spatialFilterPredicate ~ (withIndexClause?) ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ pred ~ idx => SpatialFilter(Pipe(out), Pipe(in), pred, idx)
  }


  def indexMethod = rtreeKeyword ~ "(" ~ repsep(params, ",") ~ ")" ^^ {
    case method ~ _ ~ paramList ~ _ => (IndexMethod.withName(method.toUpperCase()), paramList)
  }


  def indexStmt: Parser[PigOperator] = bag ~ "=" ~ indexKeyword ~ bag ~ onKeyword ~ ref ~ usingKeyword ~ indexMethod  ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ field ~ _ ~ methodWithParams => IndexOp(Pipe(out), Pipe(in), field, methodWithParams._1, methodWithParams._2)
  }
  
  lazy val partitionKeyword = "partition".ignoreCase
  lazy val gridKeyword = "grid".ignoreCase
  lazy val bspKeyword = "bsp".ignoreCase
  
  def partitionMethod = (gridKeyword | bspKeyword) ~ "(" ~ repsep(params, ",") ~ ")" ^^ {
    case method ~ _ ~ paramList ~ _ => (PartitionMethod.withName(method.toUpperCase()), paramList)
  }
  
  def partitionStmt = bag ~ "=" ~ partitionKeyword ~ bag ~ onKeyword ~ ref ~ usingKeyword ~ partitionMethod ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ field ~ _ ~ ((method, params)) => Partition(Pipe(out), Pipe(in), field, method, params)
  } 

  /* ---------------------------------------------------------------------------------------------------------------- */


  /* ------------------------------------------------------------ */
  /*        Pig profiling
   * ------------------------------------------------------------
   */
  import scala.concurrent.duration._

  lazy val delayKeyword = "delay".ignoreCase

  def delayStmt = bag ~ "=" ~ delayKeyword ~ bag ~ byKeyword ~ "(" ~ (pigStringLiteral|num) ~ "," ~ num ~ ")" ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ _ ~ wTime ~ _ ~ sample ~ _ =>




      val waitDur= wTime match {
        case s: String =>
          val wTimeVal = s"PT${unquote(s)}"
          Duration.fromNanos(java.time.Duration.parse(wTimeVal).toNanos)
        case i: Int =>
          i.milliseconds
      }



      Delay(Pipe(out), Pipe(in), sample, waitDur)
  }


  /* ---------------------------------------------------------------------------------------------------------------- */

  def pigStmt: Parser[PigOperator] = (
    defineMacroStmt ^^ (op => op)
      | embedStmt ^^ (op => op)
      | delimStmt ~ rep(";") ^^{ case op ~ _ => op}
    )

  def pigScript: Parser[List[PigOperator]] = rep(pigStmt)

  def parseScript(input: CharSequenceReader): List[PigOperator] = {
    phrase(pigScript)(input) match {
  	  case Success(t, _) => t
  	  case NoSuccess(msg, next) =>
  	  throw new IllegalArgumentException(s"Could not parse input string:\n${next.pos.longString} => $msg")
	  }
  }
}

object PigParser {
  def parseScript(s: CharSequence, resetSchema: Boolean = true): List[PigOperator] = {
    if (resetSchema)
      Schema.init()
    val parser = new PigParser()
    parser.parseScript(new CharSequenceReader(s))
  }

}
