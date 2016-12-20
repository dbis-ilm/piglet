package dbis.piglet.tools

import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by kai on 18.11.15.
  */
class CodeMatcherSpec extends FlatSpec with Matchers {
  "The SnippetMatcher" should "match two equal strings" in {
    SnippetMatcher.matches("abc 12 def", "abc 12 def") should be (true)
  }

  it should "not match two different strings" in {
    SnippetMatcher.matches("abc 12 def", "abc 12") should be (false)
  }

  it should "match a string to a corresponding template" in {
    SnippetMatcher.matches("abc x_8_ def x_8_", "abc x_$1_ def x_$1_") should be (true)
  }

  it should "match a string to another corresponding template" in {
    SnippetMatcher.matches("abc x_6_ def x_7_", "abc x_$2_ def x_$1_") should be (true)
  }

  it should "match a string with longer ids to a corresponding template" in {
    SnippetMatcher.matches("abc x_82_ def x_82_", "abc x_$1_ def x_$1_") should be (true)
  }

  it should "match a string with different longer ids to a corresponding template" in {
    SnippetMatcher.matches("abc x_82_ def x_83_", "abc x_$1_ def x_$2_") should be (true)
  }

  it should "not match a string to a wrong template" in {
    SnippetMatcher.matches("abc x_8_ def x_9_", "abc x_$1_ def x_$1 _") should be (false)
  }
}

