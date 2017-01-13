package dbis.piglet

import dbis.piglet.codegen.PigletCompiler
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by kai on 13.07.15.
 */
class CompilerSpec extends FlatSpec with Matchers {
  "The compiler" should "substitute parameters in a source line" in {
    val source = """a = FOREACH b GENERATE $0 AS $P1, myFunc($1) AS $PARAM2;"""
    val substitutedLine = PigletCompiler.replaceParameters(source, Map("P1" -> "column", "PARAM2" -> "funcResult"))
    substitutedLine should be ("""a = FOREACH b GENERATE $0 AS column, myFunc($1) AS funcResult;""")
  }

  it should "resolve IMPORT statements recursively" in {
    val source = List("IMPORT 'src/it/resources/import1.pig';", "C = FOREACH B GENERATE $0;")
    val (output, p) = PigletCompiler.resolveImports(source.toIterator)
    output.mkString("\n") should be (
      """A = LOAD 'input';
        |B = FILTER A BY $0 > 10;
        |C = FOREACH B GENERATE $0;""".stripMargin)
  }
}
