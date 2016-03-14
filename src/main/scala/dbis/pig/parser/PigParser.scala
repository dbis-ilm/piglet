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
package dbis.pig.parser

import dbis.pig.op._
import dbis.pig.op.cmd._
import dbis.pig.expr._
import dbis.pig.plan.DataflowPlan
import dbis.pig.schema._

import java.net.URI

import dbis.pig.tools.logging.PigletLogging

import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.parsing.input.CharSequenceReader
import scala.language.implicitConversions
import scala.language.postfixOps

/**
 * An enumeration type representing the various language sets
 * supported by the Pig compiler.
 */
object LanguageFeature extends Enumeration {
  type LanguageFeature = Value
  val PlainPig,    // standard Pig conforming to Apache Pig
  SparqlPig,       // Pig + SPARQL extensions (TUPLIFY, BGP filter)
  StreamingPig,    // Pig for data stream processing
  ComplexEventPig, // Pig for complex event processing
  CompletePiglet   // all dialects
    = Value
}

import dbis.pig.parser.LanguageFeature._

/**
 * A parser for the (extended) Pig language.
 */
class PigParser(val featureList: List[LanguageFeature] = List(PlainPig)) extends JavaTokenParsers with PigletLogging {

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
  def namedFieldWithoutLineage: Parser[NamedField] =  bag ^^ { case i => NamedField(i) }
  def namedFieldWithLineage: Parser[NamedField] = rep1sep(bag, Field.lineageSeparator) ^^
    { case l  => NamedField.fromStringList(l) }

  def literalField: Parser[Ref] =
    decimalNumber ^^ { i => if (i.contains('.')) Value(i.toDouble) else Value(i.toInt) } |
    // floatingPointNumber ^^ { n => Value(n.toDouble) } |
    stringLiteral ^^ { s => Value(s) } |
    boolean ^^ { b => Value(b) }

  def fieldSpec: Parser[Ref] = (namedField | posField | literalField)
  /*
   * A reference can be also a dereference operator for tuples, bags or maps.
   */
  def derefBagOrTuple: Parser[Ref] = (namedField | posField) ~ "." ~ (posField | namedField) ^^ { case r1 ~ _ ~ r2 => DerefTuple(r1, r2) }
  def derefMap: Parser[Ref] = (namedField | posField) ~ "#" ~ stringLiteral ^^ { case m ~ _ ~ k => DerefMap(m, k) }

  def ref: Parser[Ref] = (derefMap |  derefBagOrTuple | fieldSpec) // ( fieldSpec | derefBagOrTuple | derefMap)

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
    "int" ^^ { _ => Types.IntType }
      | "long" ^^ { _ => Types.LongType }
      | "float" ^^ { _ => Types.FloatType }
      | "double" ^^ { _ => Types.DoubleType }
      | "boolean" ^^ { _ => Types.BooleanType }
      | "chararray" ^^ { _ => Types.CharArrayType }
      | "bytearray" ^^{ _ => Types.ByteArrayType }
      | "tuple" ~ "(" ~ repsep(castTypeSpec, ",") ~ ")" ^^{
            case _ ~ _ ~ typeList ~ _ => TupleType(typeList.map(t => Field("", t)).toArray)
        }
      /*
       * bag schema: bag{tuple(<list of types>)}
       */
     // | "bag" ~ "{" ~ "tuple" ~ "(" ~ ")" ~ repsep(castTypeSpec, ",") ~ "}" ^^{ case _ ~ "{" ~ tup ~ "}" => BagType("", tup) }
      /*
       * map schema: map[<list of types>]
       */
      | "map" ~ "[" ~ "]" ^^{ case _ ~ _ ~ _ => MapType(Types.ByteArrayType) }
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
    case s ~ _ ~ rows ~ _ ~ cols ~ _ ~ expr ~ _ => ConstructMatrixExpr(s.substring(0, 2), rows, cols, expr)
  }
  
  
  
  def typeConstructor: Parser[ArithmeticExpr] = (tupleConstructor | bagConstructor | mapConstructor | matrixConstructor | geometryConstructor)

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
    case a ~ op ~ (b: String) => {
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
  }

  def boolLiteral: Parser[Predicate] = boolean ^^ { b => BoolLiteral(b)}

  def boolFactor: Parser[Predicate] = (
    boolLiteral
      | comparisonExpr ^^ { e => e }
      | "(" ~ boolExpr ~ ")" ^^ { case _ ~ e ~ _ => PPredicate(e) }
      | func ^^ { f => Eq(f,RefExpr(Value(true))) }
    )

  def boolNotFactor: Parser[Predicate] = opt(notKeyword) ~ boolFactor ^^ {
    case Some(n) ~ f => Not(f); case None ~ f => f
  }

  def boolTerm: Parser[Predicate] = boolNotFactor ~ rep(andKeyword ~ boolNotFactor) ^^{
    case f ~ list => list.foldLeft(f) {
        case (p1, andKeyword ~ p2) => And(p1, p2)
      }
  }

  def boolExpr: Parser[Predicate] = boolTerm ~ rep(orKeyword ~ boolTerm) ^^{
    case f ~ list => list.foldLeft(f) {
      case (p1, orKeyword ~ p2) => Or (p1, p2)
    }
  }

  /*
   * The list of case-insensitive keywords we want to accept.
   */
  lazy val loadKeyword = "load".ignoreCase
  lazy val dumpKeyword = "dump".ignoreCase
  lazy val displayKeyword = "display".ignoreCase
  lazy val storeKeyword = "store".ignoreCase
  lazy val intoKeyword = "into".ignoreCase
  lazy val filterKeyword = "filter".ignoreCase
  lazy val byKeyword = "by".ignoreCase
  lazy val groupKeyword = "group".ignoreCase
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

  
  
  def boolean: Parser[Boolean] = (
      trueKeyword ^^ { _=> true }
      | falseKeyword ^^ { _ => false }
    )
  
  /*
   * tuple schema: tuple(<list of fields>) or (<list of fields>)
   */
  def tupleTypeSpec: Parser[TupleType] =
    ("tuple"?) ~ "(" ~repsep(fieldSchema, ",") ~ ")" ^^{ case _ ~ _ ~ fieldList ~ _ => TupleType(fieldList.toArray) }

  def matrixTypeName: Parser[String] = "[sd][id]matrix".r
  def matrixTypeSpec: Parser[PigType] = matrixTypeName ~ "(" ~ num ~ "," ~ num ~ ")" ^^{
    case n ~ _ ~ rows ~ _ ~ cols ~ _ =>
      val t = if (n.charAt(1) == 'i') Types.IntType else Types.DoubleType
      val rep = if (n.charAt(0) == 's') MatrixRep.SparseMatrix else MatrixRep.DenseMatrix
      MatrixType(t, rows, cols, rep)
  }
  
//  def geometryTypeSpec: Parser[GeometryType] = geometryTypeName ~ "(" ~ stringLiteral ~ ")" ^^ {
//    case _ ~ _ ~ wkt ~ _ =>  GeometryType(wkt)
//    
//  }

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
    | ("bag"?) ~ "{" ~ tupleTypeSpec ~ "}" ^^{ case _ ~  _ ~ tup ~ _ => BagType(tup) }
      /*
       * map schema: map[<list of fields>] or [<list of fields>]
       */
    | ("map"?) ~ "[" ~(typeSpec?) ~ "]" ^^{ case _ ~ _ ~ ty ~ _ => ty match {
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

  def usingClause: Parser[(String, List[String])] = usingKeyword ~ ident ~ "(" ~ repsep(pigStringLiteral, ",") ~ ")" ^^ {
    case _ ~ loader ~ _ ~ params ~ _ => (loader, params)
  }

  def fieldRef: Parser[Ref] = posField | namedFieldWithoutLineage
  def timestampClause: Parser[Ref] = timestampKeyword ~ "(" ~ fieldRef ~ ")" ^^ { case _ ~ _ ~ f ~ _ => f }

  def loadStmt: Parser[PigOperator] = bag ~ "=" ~ loadKeyword ~ fileName ~ (usingClause?) ~ (loadSchemaClause?) ~ (timestampClause?) ^^ {
    case b ~ _ ~ _ ~ f ~ u ~ s ~ ts =>
      val uri = new URI(f)
      if (s.isDefined && ts.isDefined) ts.get match {
        case NamedField(n, _) => s.get.timestampField = s.get.indexOfField(n)
        case PositionalField(p) => s.get.timestampField = p
        case _ => {}
      }
      u match {
        case Some(p) => new Load(Pipe(b), uri, s, Some(p._1), if (p._2.isEmpty) null else p._2.map(s => s""""${unquote(s)}""""))
        case None => new Load(Pipe(b), uri, s)
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

  /*
   * DUMP <A>
   */
  def dumpStmt: Parser[PigOperator] = dumpKeyword ~ bag ^^ { case _ ~ b => new Dump(Pipe(b)) }

  /*
   * DISPLAY <A>
   */
  def displayStmt: Parser[PigOperator] = displayKeyword ~ bag ^^ { case _ ~ b => new Display(Pipe(b)) }

  /*
   * STORE <A> INTO "<FileName>"
   */
  def storeStmt: Parser[PigOperator] = storeKeyword ~ bag ~ intoKeyword ~ fileName ~ (usingClause?) ^^ { 
    case _ ~ b ~  _ ~ f ~ u => 
      val uri = new URI(f)
      u match {
        case Some(p) => new Store(Pipe(b), uri, Some(p._1), if(p._2.isEmpty) null else p._2.map(s => s""""${unquote(s)}""""))
        case None => new Store(Pipe(b), uri)
      }
  }

  /*
   * GENERATE expr1, expr2, ...
   */
  def generateStmt: Parser[PigOperator] = plainForeachGenerator ^^ { case g => new Generate(g.asInstanceOf[GeneratorList].exprs) }

  /*
   * B = A.C;
   */
  def constructBagStmt: Parser[PigOperator] = bag ~ "=" ~ derefBagOrTuple ^^ { case res ~ _ ~ ref => new ConstructBag(Pipe(res), ref) }

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
  def asteriskExpr: Parser[GeneratorExpr] = "*" ^^ { case _ => GeneratorExpr(RefExpr(NamedField("*"))) }
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
      case out ~ _ ~ _ ~ in ~ ex => new Foreach(Pipe(out), Pipe(in), ex)
    }

  /*
   * <A> = ACCUMULATE <B> GENERATE <Expr> [ AS <Schema> ]
   */
  def accumulateStmt: Parser[PigOperator] = bag ~ "=" ~ accumulateKeyword ~ bag ~ generateKeyword ~ generatorList ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ exList => new Accumulate(Pipe(out), Pipe(in), GeneratorList(exList))
  }

  /*
   * <A> = FILTER <B> BY <Predicate>
   */
  def filterStmt: Parser[PigOperator] = bag ~ "=" ~ filterKeyword ~ bag ~ byKeyword ~ boolExpr ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ pred => new Filter(Pipe(out), Pipe(in), pred)
  }

  /*
   * DESCRIBE <A>
   */
  def describeStmt: Parser[PigOperator] = describeKeyword ~ bag ^^ { case _ ~ b => new Describe(Pipe(b)) }

  /*
   * <A> = GROUP <B> ALL
   * <A> = GROUP <B> BY <Ref>
   * <A> = GROUP <B> B> ( <ListOfRefs> )
   */
  def refList: Parser[List[Ref]] = (ref ^^ { r => List(r) } | "(" ~ repsep(ref, ",") ~ ")" ^^ { case _ ~ rlist ~ _ => rlist})
  def groupingClause: Parser[GroupingExpression] = allKeyword ^^ { s => GroupingExpression(List())} |
    (byKeyword ~ refList ^^ { case _ ~ rlist => GroupingExpression(rlist)})
  def groupingStmt: Parser[PigOperator] = bag ~ "=" ~ groupKeyword ~ bag ~ groupingClause ^^ {
    case out ~ _ ~ _ ~ in ~ grouping => new Grouping(Pipe(out), Pipe(in), grouping) }

  /*
   * <A> = DISTINCT <B>
   */
  def distinctStmt: Parser[PigOperator] = bag ~ "=" ~ distinctKeyword ~ bag ^^ { case out ~ _ ~ _ ~ in => new Distinct(Pipe(out), Pipe(in), false) }

  /*
   * <A> = LIMIT <B> <Num>
   */
  def limitStmt: Parser[PigOperator] = bag ~ "=" ~ limitKeyword ~ bag ~ num ^^ { case out ~ _ ~ _ ~ in ~ num => new Limit(Pipe(out), Pipe(in), num) }

  /*
   * <A> = JOIN <B> BY <Ref>, <C> BY <Ref>, ...
   * <A> = JOIN <B> BY ( <ListOfRefs> ), <C> BY ( <ListOfRefs>), ...
   */
  def joinExpr: Parser[(String, List[Ref])] = bag ~ byKeyword ~ refList ^^ { case b ~ _ ~ rlist => (b, rlist) }
  def joinExprList: Parser[List[(String, List[Ref])]] = repsep(joinExpr, ",") ^^ { case jlist => jlist }
  def extractJoinRelation(jList: List[(String, List[Ref])]): List[Pipe] = { jList.map{ case (alias, refs) => Pipe(alias) } }
  def extractJoinFields(jList: List[(String, List[Ref])]): List[List[Ref]] = { jList.map{ case (alias, refs) => refs } }
  def joinStmt: Parser[PigOperator] = bag ~ "=" ~ joinKeyword ~ joinExprList ^^ {
    case out ~ _ ~ _ ~ jlist => new Join(Pipe(out), extractJoinRelation(jlist), extractJoinFields(jlist)) }
  
  /*
   * <A> = CROSS <B>, <C>, ...
   */
  def crossStmt: Parser[PigOperator] = bag ~ "=" ~ crossKeyword ~ repsep(bag, ",") ^^ {
    case out ~ _ ~ _ ~ rlist => new Cross(Pipe(out), rlist.map(r => Pipe(r)))
  }

  /*
   * <A> = UNION <B>, <C>, <D>, ...
   */
  def unionStmt: Parser[PigOperator] = bag ~ "=" ~ unionKeyword ~ repsep(bag, ",") ^^ {
    case out ~ _ ~ _ ~ rlist => new Union(Pipe(out), rlist.map(r => Pipe(r)))
  }

  /*
   * REGISTER <JarFile>
   */
  def registerStmt: Parser[PigOperator] = registerKeyword ~ fileName ^^{ case _ ~ uri => new RegisterCmd(uri) }

  /*
   * DEFINE <Alias> <FuncName>
   */
  def defineStmt: Parser[PigOperator] = defineKeyword ~ ident ~ className ~ "(" ~ repsep(literalField, ",") ~ ")" ^^{
    case _ ~ alias ~ funcName ~ _ ~ params ~ _ => DefineCmd(alias, funcName, params.map(r => r.asInstanceOf[dbis.pig.expr.Value]))
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
  def setStmt: Parser[PigOperator] = setKeyword ~ ident ~ literalField ^^ {
    case _ ~ k ~ v => SetCmd(k, v.asInstanceOf[dbis.pig.expr.Value]) }

  /*
   * <A> = STREAM <B> TROUGH <Operator> [(ParamList)] [AS (<Schema>) ]
   */
  def streamStmt: Parser[PigOperator] = bag ~ "=" ~ streamKeyword ~ bag ~ throughKeyword ~ className ~ (paramList?) ~ (loadSchemaClause?) ^^{
    case out ~ _ ~_ ~ in ~ _ ~ opname ~ params ~ schema => new StreamOp(Pipe(out), Pipe(in), opname, params, schema)
  }

  /*
   * <A> = SAMPLE <B> <num>
   * <A> = SAMPLE <B> <Expr>
   */
  def sampleStmt: Parser[PigOperator] = bag ~ "=" ~ sampleKeyword ~ bag ~ arithmExpr ^^ {
    case out ~ _ ~ _ ~ in ~ expr => new Sample(Pipe(out), Pipe(in), expr)
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
    case out ~ _ ~ _ ~ in ~ _ ~ spec => new OrderBy(Pipe(out), Pipe(in), spec)
  }

  /*
   * SPLIT <A> INTO <B> IF <Cond>, <C> IF <Cond> ...
   */
  def splitBranch: Parser[SplitBranch] = bag ~ ifKeyword ~ boolExpr ^^ { case out ~ _ ~ expr => new SplitBranch(Pipe(out), expr)}

  def splitStmt: Parser[PigOperator] = splitKeyword ~ bag ~ intoKeyword ~ repsep(splitBranch, ",") ^^ {
    case _ ~ in ~ _ ~ splitList => new SplitInto(Pipe(in), splitList)
  }

  /*
   * MATERIALIZE <A>
   */
  def materializeStmt: Parser[PigOperator] = materializeKeyword ~ bag ^^ { case _ ~ b => Materialize(Pipe(b))}

  def rscriptStmt: Parser[PigOperator] = bag ~ "=" ~ rscriptKeyword ~ bag ~ usingKeyword ~ pigStringLiteral ~ (loadSchemaClause?) ^^{
    case out ~ _ ~ _ ~ in ~ _ ~ script ~ schema => new RScript(Pipe(out), Pipe(in), script, schema)
  }

  /*
   * fs -cmd params
   */
  def fsCmd: Parser[String] = ("""-[a-zA-Z]*""").r ^^ { s => s.substring(1) }
  def fsParam: Parser[String] = ("""[^;][/\w\.\-]*""").r

  def fsStmt: Parser[PigOperator] = fsKeyword ~ fsCmd ~ rep(fsParam) ^^ { case _ ~ cmd ~ params => HdfsCmd(cmd, params)}

  def code = ("""(?s)(.*?)""")

  def embeddedCode: Parser[EmbedCmd] = "<%" ~ (code + "%>").r ^^ {
    case _ ~ code if code.split("rules:").length != 2 => new EmbedCmd(code.substring(0, code.length - 2))
    case _ ~ code =>
      val ctuple = code.substring(0, code.length -2 ).split("rules:")
      new EmbedCmd(ctuple(0), Some(ctuple(1)))
  }

  def embedStmt: Parser[PigOperator] = embeddedCode

  /*
   * A statement can be one of the above delimited by a semicolon.
   */
  def delimStmt: Parser[PigOperator] = (loadStmt | dumpStmt | describeStmt | foreachStmt | filterStmt | groupingStmt | accumulateStmt |
    distinctStmt | joinStmt | spatialJoinStmt | crossStmt | storeStmt | limitStmt | unionStmt | registerStmt | streamStmt | sampleStmt | orderByStmt |
    splitStmt | materializeStmt | rscriptStmt | fsStmt | defineStmt | setStmt | macroRefStmt | displayStmt )

  /* ---------------------------------------------------------------------------------------------------------------- */
  /*
   * Pig extensions for processing RDF data and supporting SPARQL BGPs.
   */
  lazy val tuplifyKeyword = "tuplify".ignoreCase
  lazy val onKeyword = "on".ignoreCase
  lazy val bgpFilterKeyword = "bgp_filter".ignoreCase

  def tuplifyStmt: Parser[PigOperator] = bag ~ "=" ~ tuplifyKeyword ~ bag ~ onKeyword ~ ref ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ r => new Tuplify(Pipe(out), Pipe(in), r) }

  def bgpVariable: Parser[NamedField] = "?" ~ ident ^^ { case _ ~ varname => NamedField(varname)}

  def bgPattern: Parser[TriplePattern] = (bgpVariable | ref) ~(bgpVariable | ref) ~ (bgpVariable | ref) ^^ {
    case r1 ~ r2 ~ r3 => TriplePattern(r1, r2, r3)
  }

  def bgpFilterStmt: Parser[PigOperator] = bag ~ "=" ~ bgpFilterKeyword ~ bag ~
    byKeyword ~ "{" ~ repsep(bgPattern, ".") ~ "}" ^^ {
    case out ~ _ ~ _ ~ in ~ _ ~ _ ~ pattern ~ _ => new BGPFilter(Pipe(out), Pipe(in), pattern)
  }

  def sparqlStmt: Parser[PigOperator] = (tuplifyStmt | bgpFilterStmt | rdfLoadStmt)

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
  def timeUnit: Parser[String] = ("seconds".ignoreCase | "minutes".ignoreCase) //^^ { _.toUpperCase }
  def rangeParam: Parser[Tuple2[Int,String]] = rangeKeyword ~ num ~ timeUnit ^^ {case _ ~ n ~ u => (n,u)}
  def rowsParam: Parser[Tuple2[Int,String]] = rowsKeyword ~ num ^^ {case _ ~ n => (n, "")}
  def windowParam: Parser[Tuple2[Int,String]] = (rangeParam | rowsParam)
  def windowStmt: Parser[PigOperator] = bag ~ "=" ~ windowKeyword ~ bag ~ windowParam ~ slideKeyword ~ windowParam ^^ {
    case out ~ _ ~ _ ~ in ~ on ~ _ ~ slide => new Window(Pipe(out), Pipe(in), on, slide)
  } | bag ~ "=" ~ windowKeyword ~ bag ~ windowParam ^^ {
    case out ~ _ ~ _ ~ in ~ on => new Window(Pipe(out), Pipe(in), on, on)
  }

  /*
   * Socket Definitions
   */
  def ipMember: Parser[String] = "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)".r
  def ipv4: Parser[String] = ipMember ~ "." ~ ipMember ~ "." ~ ipMember ~ "." ~ ipMember ^^{
    case i1 ~ _ ~ i2 ~ _ ~ i3 ~ _ ~i4 => i1 + "." + i2 + "." + i3 + "." + i4
  }
  def portNum: Parser[String] = "([0-9]{1,5})".r
  def port: Parser[String] = (portNum | "*")
  def bindAddress: Parser[String] = (ipv4 | "*" | ident)

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
          case _ => {}
        }
        u match {
          case Some(p) => new SocketRead(Pipe(out), addr, "", schema, Some(p._1), if (p._2.isEmpty) null else p._2.map(s => s""""${unquote(s)}""""))
          case None =>  new SocketRead(Pipe(out), addr, "", schema)
      }

    } |
      bag ~ "=" ~ socketReadKeyword ~ zmqAddress ~ modeKeyword ~ zmqKeyword ~ (usingClause?) ~ (loadSchemaClause?) ~ (timestampClause?) ^^ {
        case out ~ _ ~ _ ~ addr ~ _ ~ mode ~ u ~ schema ~ ts =>
          if (schema.isDefined && ts.isDefined) ts.get match {
            case NamedField(n, _) => schema.get.timestampField = schema.get.indexOfField(n)
            case PositionalField(p) => schema.get.timestampField = p
            case _ => {}
          }
          u match {
          case Some(p) => new SocketRead(Pipe(out), addr, mode, schema, Some(p._1), if (p._2.isEmpty) null else p._2.map(s => s""""${unquote(s)}""""))
          case None => new SocketRead(Pipe(out), addr, mode, schema)
        }
      }

  /*
   * SOCKET_WRITE <A> TO '<address>' [ MODE ZMQ ] 
   */
  def socketWriteStmt: Parser[PigOperator] =
    socketWriteKeyword ~ bag ~ toKeyword ~ inetAddress ~ (usingClause?) ^^ {
      case _ ~ b ~ _ ~ addr ~ u => u match {
        case Some(p) => new SocketWrite(Pipe(b), addr, "", Some(p._1), if(p._2.isEmpty) null else p._2.map(s => s""""${unquote(s)}""""))
        case None =>  new SocketWrite(Pipe(b), addr)
      }
    } |
      socketWriteKeyword ~ bag ~ toKeyword ~ zmqAddress ~ modeKeyword ~ zmqKeyword ~ (usingClause?) ^^ {
        case _ ~ b ~ _ ~ addr ~ _ ~ mode ~ u => u match {
          case Some(p) => new SocketWrite(Pipe(b), addr, mode, Some(p._1), if(p._2.isEmpty) null else p._2.map(s => s""""${unquote(s)}""""))
          case None =>  new SocketWrite(Pipe(b), addr, mode)
        }
      }
  
  def streamingStmt: Parser[PigOperator] =  (socketReadStmt | socketWriteStmt | windowStmt)

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
  def simplePattern: Parser[Pattern] = ident ^^ { case s => SimplePattern(s) }
  def patternParam: Parser[Pattern] = (seqPattern | negPattern | conjPattern | disjPattern | simplePattern)
  
  def withinParam: Parser[Tuple2[Int, String]] = withinKeyword ~ num ~ timeUnit ^^ { case _ ~ n ~ u => (n, u) }
  def modes: Parser[String] = (skipNextKeyword | skipAnyKeyword | firstMatchKeyword | recentMatchKeyword | cognitiveMatchKeyword)
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

  def complexEventStmt: Parser[PigOperator] = matcherStmt

  /* ---------------------------------------------------------------------------------------------------------------- */

  

  /* ------------------------------------------------------------ */
  /*        Pig extensions for spatial data
   * ------------------------------------------------------------
   */
  
  lazy val geometryTypeName = "geometry".ignoreCase
  lazy val spatialJoinKeyword = "SPATIALJOIN".ignoreCase
  lazy val containsKeyword = "contains".ignoreCase
  lazy val intersectsKeyword = "intersects".ignoreCase
  
  def geometryConstructor: Parser[ArithmeticExpr] = geometryTypeName ~ "(" ~ arithmExpr ~ ")" ^^ {
    case _ ~ _ ~ exp ~ _ => ConstructGeometryExpr(exp)
  }
  
  import SpatialPredicateType._
  
  def containsPredicate = containsKeyword ~ "(" ~ ref ~ "," ~ ref ~ ")" ^^ {    
    case _ ~ _ ~ r1 ~ _ ~ r2 ~ _ => new SpatialPredicate(r1, r2, SpatialPredicateType.CONTAINS)
  }
  
  def intersectsPredicate = intersectsKeyword ~ "(" ~ ref ~ "," ~ ref ~ ")" ^^ {    
    case _ ~ _ ~ r1 ~ _ ~ r2 ~ _ => new SpatialPredicate(r1, r2, SpatialPredicateType.INTERSECTS)
  } 
  
  def spatialPredicate = containsPredicate | intersectsPredicate
  
  
  def spatialJoinStmt: Parser[PigOperator] = bag ~ "=" ~ spatialJoinKeyword ~ bag ~ "," ~ bag ~ onKeyword ~ spatialPredicate ^^ {
    case out ~ _ ~ _ ~ in1 ~ _ ~ in2  ~ _ ~ expr => new SpatialJoin(Pipe(out), List(Pipe(in1), Pipe(in2)), expr)
  }
  
  /* ---------------------------------------------------------------------------------------------------------------- */
  
  
  def langStmt(features: List[LanguageFeature]): Parser[PigOperator] = {
    def parseStmt(feature: LanguageFeature) = {
      feature match {
        case PlainPig => delimStmt
        case StreamingPig => streamingStmt
        case SparqlPig => sparqlStmt
        case ComplexEventPig => complexEventStmt
      }
    }

    if (features.tail.isEmpty)
      parseStmt(features.head)
    else
      parseStmt(features.head) | langStmt(features.tail)
  }

  def pigStmt: Parser[PigOperator] = (
    defineMacroStmt ^^{ case op => op}
      | embedStmt ^^{ case op => op}
      | langStmt(featureList) ~ rep(";") ^^{ case op ~ _ => op}
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
  /**
    * Processes a list of language features accepted by the parser. It ensures that
    * always PlainPig appears at the and of the list. Furthermore, the pseudo feature
    * CompletePiglet is replaced by the list of all features.
    *
    * @param features the input feature list
    * @return a correct feature list
    */
  def handleFeatureSet(features: List[LanguageFeature]): List[LanguageFeature] = {
    if (features.contains(CompletePiglet))
      List(ComplexEventPig, StreamingPig, SparqlPig, PlainPig)
    else if (! features.contains(PlainPig))
      features ::: List(PlainPig)
    else
      features.filter(_ != PlainPig) ::: List(PlainPig)
  }

  def parseScript(s: CharSequence, featureList: List[LanguageFeature] = List(PlainPig),
                  resetSchema: Boolean = true): List[PigOperator] = {
    if (resetSchema)
      Schema.init()
    val parser = new PigParser(handleFeatureSet(featureList))
    parser.parseScript(new CharSequenceReader(s))
  }

}
