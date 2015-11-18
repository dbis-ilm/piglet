package dbis.test

import org.scalatest._
import matchers._

import scala.collection.mutable

/**
  * Created by kai on 18.11.15.
  */
trait CodeMatchers {

  /**
    * A matcher for code snippets which allows to compare generated code with a template string
    * where certain parameters denoted by $1 ... $9 can be matched to arbitrary numbers (actually digits).
    * Note that the same template parameter matches to the same value, but different parameters
    * expect different values. Examples:
    *
    * "abc $1 def $1" matches "abc 1 def 1" but not "abc 7 def 8"
    * "abc $1 def $2" matches "abc 8 def 9"
    *
    * @param expectedStringTemplate the code template containing parameters $1 ... $9
    */
  class CodeSnippetMatcher(expectedStringTemplate: String) extends Matcher[String] {

     def apply(left: String) = {
      MatchResult(
        SnippetMatcher.matches(left, expectedStringTemplate),
        s"""String $left did not match template "$expectedStringTemplate"""",
        s"""String $left matches template extension "$expectedStringTemplate""""
      )
    }
  }

  def matchSnippet(expectedStringTemplate: String) = new CodeSnippetMatcher(expectedStringTemplate)
}

/**
  * A singleton class implementing the code snippet matching. This functions is outsourced only to
  * allow to test it.
  */
object SnippetMatcher {
  def matches(snippet: String, template: String): Boolean = {
    val replacements = mutable.Map[String, String]()
    val pattern = "\\$[0-9]".r
    val positions = pattern.findAllMatchIn(template)
      .map(p => p.start)
      .zipWithIndex
      .map{ case (p, offset) => p - offset}.toList
    val keys = pattern.findAllMatchIn(template).map(p => p.toString).toList
    for (i <- 0 until keys.length) {
      replacements += (keys(i) -> snippet(positions(i)).toString)
    }
    var s = template
    replacements.foreach{case (k, v) => s = s.replace(k, v)}
    snippet == s
  }
}

// Make them easy to import with:
// import CodeMatchers._
object CodeMatchers extends CodeMatchers
