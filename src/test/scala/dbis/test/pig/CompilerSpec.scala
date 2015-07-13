package dbis.test.pig

import dbis.pig.PigCompiler
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by kai on 13.07.15.
 */
class CompilerSpec extends FlatSpec with Matchers {

  "The compiler" should "substitute parameters in a source line" in {
    val source = """a = FOREACH b GENERATE $0 AS $P1, myFunc($1) AS $PARAM2;"""
    val substitutedLine = PigCompiler.replaceParameters(source, Map("P1" -> "column", "PARAM2" -> "funcResult"))
    substitutedLine should be ("""a = FOREACH b GENERATE $0 AS column, myFunc($1) AS funcResult;""")
  }
}
