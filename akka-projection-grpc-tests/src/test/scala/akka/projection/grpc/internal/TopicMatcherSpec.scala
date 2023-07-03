/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.internal

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class TopicMatcherSpec extends AnyWordSpecLike with Matchers {

  "TopicMatcher" should {
    "match all wildcard" in {
      val matcher = TopicMatcher("#")
      matcher.matches("myhome/groundfloor/livingroom/temperature") shouldBe true
    }

    "match exact" in {
      val matcher = TopicMatcher("myhome/groundfloor")
      matcher.matches("myhome/groundfloor") shouldBe true
      matcher.matches("otherhome/groundfloor") shouldBe false
      matcher.matches("myhome/groundfloor/kitchen") shouldBe false
      matcher.matches("Myhome/groundfloor") shouldBe false
    }

    "match single level wildcard" in {
      val matcher = TopicMatcher("myhome/groundfloor/+/temperature")
      matcher.matches("myhome/groundfloor/livingroom/temperature") shouldBe true
      matcher.matches("myhome/groundfloor/kitchen/brightness") shouldBe false
      matcher.matches("myhome/firstfloor/kitchen/temperature") shouldBe false
      matcher.matches("myhome/groundfloor/kitchen/fridge/temperature") shouldBe false
      matcher.matches("myhome/groundfloor") shouldBe false
      matcher.matches("myhome/groundfloor/livingroom/temperature/") shouldBe true
    }

    "match with only one single level wildcard" in {
      val matcher = TopicMatcher("+")
      matcher.matches("myhome") shouldBe true
      matcher.matches("myhome/groundfloor") shouldBe false
      matcher.matches("/myhome") shouldBe false
      matcher.matches("myhome/") shouldBe true
    }

    "match with several single level wildcards" in {
      val matcher = TopicMatcher("+/groundfloor/+/temperature")
      matcher.matches("myhome/groundfloor/livingroom/temperature") shouldBe true
      matcher.matches("myhome/groundfloor/kitchen/brightness") shouldBe false

      matcher.matches("otherhome/groundfloor/livingroom/temperature") shouldBe true
      matcher.matches("otherhome/groundfloor/kitchen/brightness") shouldBe false
    }

    "match multi level wildcard" in {
      val matcher = TopicMatcher("myhome/groundfloor/#")
      matcher.matches("myhome/groundfloor/livingroom/temperature") shouldBe true
      matcher.matches("myhome/groundfloor/kitchen/brightness") shouldBe true
      matcher.matches("myhome/firstfloor/kitchen/temperature") shouldBe false

      matcher.matches("myhome/groundfloor/") shouldBe false
      matcher.matches("myhome/groundfloor//") shouldBe false
    }

    "match with single and multi level wildcards" in {
      val matcher = TopicMatcher("+/groundfloor/#")
      matcher.matches("myhome/groundfloor/livingroom/temperature") shouldBe true
      matcher.matches("myhome/groundfloor/kitchen/brightness") shouldBe true
      matcher.matches("myhome/firstfloor/kitchen/temperature") shouldBe false
      matcher.matches("otherhome/groundfloor/kitchen/brightness") shouldBe true
    }

    "not allow empty expression" in {
      intercept[IllegalArgumentException] {
        TopicMatcher.checkValid("")
      }
    }

    "not allow multi level wildcard in other place than at the end" in {
      intercept[IllegalArgumentException] {
        TopicMatcher.checkValid("myhome/#/")
      }
      intercept[IllegalArgumentException] {
        TopicMatcher.checkValid("myhome/#/temperature")
      }

      TopicMatcher.checkValid("#")
      TopicMatcher.checkValid("/#")
    }

    "only allow multi level wildcard for a full token" in {
      intercept[IllegalArgumentException] {
        TopicMatcher.checkValid("myhome/groundfloor/temp#")
      }
    }

    "only allow single level wildcard for a full token" in {
      intercept[IllegalArgumentException] {
        TopicMatcher.checkValid("myhome/g+floor/temperature")
      }
      intercept[IllegalArgumentException] {
        TopicMatcher.checkValid("myhome/ground+/temperature")
      }
      intercept[IllegalArgumentException] {
        TopicMatcher.checkValid("myhome/+floor/temperature")
      }
      intercept[IllegalArgumentException] {
        TopicMatcher.checkValid("myhome/groundfloor/temp+")
      }
      intercept[IllegalArgumentException] {
        TopicMatcher.checkValid("a+")
      }
      intercept[IllegalArgumentException] {
        TopicMatcher.checkValid("a+/")
      }

      TopicMatcher.checkValid("/+")
      TopicMatcher.checkValid("+/")
      TopicMatcher.checkValid("/+/")
    }

    "strip trailing separator chars" in {
      import TopicMatcher.Separator
      TopicMatcher.stripEnd("a/b/", Separator) shouldBe "a/b"
      TopicMatcher.stripEnd("a/b//", Separator) shouldBe "a/b"
      TopicMatcher.stripEnd("a/b///", Separator) shouldBe "a/b"
      TopicMatcher.stripEnd("a/", Separator) shouldBe "a"

      TopicMatcher.stripEnd("a/b", Separator) shouldBe "a/b"
      TopicMatcher.stripEnd("a", Separator) shouldBe "a"
      TopicMatcher.stripEnd("", Separator) shouldBe ""

      val matcher = TopicMatcher("myhome/#")
      matcher.matches("myhome/groundfloor/") shouldBe true
      matcher.matches("myhome/groundfloor//") shouldBe true
    }
  }
}
