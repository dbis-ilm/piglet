package dbis.test

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

  it should "not match a string to a wrong template" in {
    SnippetMatcher.matches("abc x_8_ def x_9_", "abc x_$1_ def x_$1 _") should be (false)
  }
}

