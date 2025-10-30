/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import java.time.Instant

import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.projection.grpc.internal.FilterStage.Filter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class FilterSpec extends AnyWordSpecLike with Matchers {

  private def createEnvelope(pid: String, tags: Set[String] = Set.empty): EventEnvelope[Any] = {
    val slice = math.abs(pid.hashCode % 1024)
    val seqNr = 1
    val now = Instant.now()
    EventEnvelope(
      TimestampOffset(Instant.now, Map(pid -> seqNr)),
      pid,
      seqNr,
      "evt",
      now.toEpochMilli,
      PersistenceId.extractEntityType(pid),
      slice,
      filtered = false,
      source = "",
      tags = tags)
  }

  "Filter" should {
    "include with empty filter" in {
      Filter.empty("t:").matches(createEnvelope("pid")) shouldBe true
    }

    "honor include after exclude" in {
      val filter =
        Filter
          .empty("t:")
          .addIncludePersistenceIds(Set("pid-1", "pid-3"))
          .addExcludePersistenceIds(Set("pid-3", "pid-2"))
          .addIncludeTags(Set("a"))
          .addExcludeTags(Set("a", "b"))
      filter.matches(createEnvelope("pid-1")) shouldBe true
      filter.matches(createEnvelope("pid-2")) shouldBe false
      filter.matches(createEnvelope("pid-3")) shouldBe true
      filter.matches(createEnvelope("pid-4", tags = Set("b"))) shouldBe false
      filter.matches(createEnvelope("pid-5", tags = Set("a"))) shouldBe true
      filter.matches(createEnvelope("pid-6", tags = Set("a", "b"))) shouldBe true
    }

    "match topics" in {
      val filter =
        Filter
          .empty("t:")
          .addExcludeRegexEntityIds(List(".*"))
          .addIncludeTopics(Set("a/+/c", "d/#"))
      filter.matches(createEnvelope("pid-1")) shouldBe false
      filter.matches(createEnvelope("pid-2", tags = Set("t:a/b/c"))) shouldBe true
      filter.matches(createEnvelope("pid-3", tags = Set("t:d/e"))) shouldBe true
      filter.matches(createEnvelope("pid-4", tags = Set("a/b/c"))) shouldBe false
    }

    "exclude with regexp" in {
      val filter =
        Filter
          .empty("t:")
          .addIncludePersistenceIds(Set("Entity|a-1", "Entity|a-2"))
          .addExcludeRegexEntityIds(List("a-.*"))
      filter.matches(createEnvelope("Entity|a-1")) shouldBe true
      filter.matches(createEnvelope("Entity|a-2")) shouldBe true
      filter.matches(createEnvelope("Entity|a-3")) shouldBe false
      filter.matches(createEnvelope("Entity|b-1")) shouldBe true
    }

    "remove criteria" in {
      val filter =
        Filter
          .empty("t:")
          .addIncludePersistenceIds(Set("pid-1", "pid-3"))
          .addExcludePersistenceIds(Set("pid-3", "pid-2"))
      filter.matches(createEnvelope("pid-1")) shouldBe true
      filter.matches(createEnvelope("pid-3")) shouldBe true
      val filter2 = filter.removeIncludePersistenceIds(Set("pid-3"))
      filter2.matches(createEnvelope("pid-3")) shouldBe false
    }
  }

}
