/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import java.time.Instant

import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class TopicMatcherSpec extends AnyWordSpecLike with Matchers {

  def createEnvelope(tags: Set[String]): EventEnvelope[String] =
    EventEnvelope(
      TimestampOffset(Instant.now, Map("test|pid" -> 1L)),
      "test|pid",
      1L,
      "evt",
      System.currentTimeMillis(),
      "test",
      0,
      filtered = false,
      source = "",
      tags)

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

    "match topic with wildcard in the topic" in {
      val matcher = TopicMatcher("myhome/groundfloor/+/temperature")
      // not recommended to use wildcard symbols in the topic names, but we can't prevent it
      // and they are like any other characters in the topic names
      matcher.matches("myhome/groundfloor/+/temperature") shouldBe true
      matcher.matches("myhome/groundfloor/living+/temperature") shouldBe true
      matcher.matches("+/groundfloor/+/temperature") shouldBe false
      matcher.matches("myhome/groundfloor/#") shouldBe false
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

    "match on topic tag in EventEnvelope" in {
      val matcher = TopicMatcher("myhome/groundfloor/+/temperature")
      matcher.matches(createEnvelope(tags = Set("t:myhome/groundfloor/livingroom/temperature")), "t:") shouldBe true
      matcher.matches(createEnvelope(tags = Set("t:myhome/groundfloor/kitchen/brightness")), "t:") shouldBe false
      matcher.matches(createEnvelope(tags = Set("other")), "t:") shouldBe false
      matcher.matches(createEnvelope(tags = Set.empty), "t:") shouldBe false
    }

    "match all even if topic tag is not defined EventEnvelope" in {
      val matcher = TopicMatcher("#")
      matcher.matches(createEnvelope(tags = Set("t:myhome/groundfloor/livingroom/temperature")), "t:") shouldBe true
      matcher.matches(createEnvelope(tags = Set("other")), "t:") shouldBe true
      matcher.matches(createEnvelope(tags = Set.empty), "t:") shouldBe true
    }
  }
}
