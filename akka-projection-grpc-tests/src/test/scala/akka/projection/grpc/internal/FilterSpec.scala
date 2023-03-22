/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.projection.grpc.internal.FilterStage.Filter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class FilterSpec extends AnyWordSpecLike with Matchers {
  "Filter" should {
    "include with empty filter" in {
      Filter.empty.matches("pid-1") shouldBe true
    }

    "honor include after exclude" in {
      val filter =
        Filter.empty.addIncludePersistenceIds(Set("pid-1", "pid-3")).addExcludePersistenceIds(Set("pid-3", "pid-2"))
      filter.matches("pid-1") shouldBe true
      filter.matches("pid-2") shouldBe false
      filter.matches("pid-3") shouldBe true
    }

    "exclude with regexp" in {
      val filter =
        Filter.empty.addIncludePersistenceIds(Set("Entity|a-1", "Entity|a-2")).addExcludeRegexEntityIds(List("a-.*"))
      filter.matches("Entity|a-1") shouldBe true
      filter.matches("Entity|a-2") shouldBe true
      filter.matches("Entity|a-3") shouldBe false
      filter.matches("Entity|b-1") shouldBe true
    }

    "remove criteria" in {
      val filter =
        Filter.empty.addIncludePersistenceIds(Set("pid-1", "pid-3")).addExcludePersistenceIds(Set("pid-3", "pid-2"))
      filter.matches("pid-1") shouldBe true
      filter.matches("pid-3") shouldBe true
      val filter2 = filter.removeIncludePersistenceIds(Set("pid-3"))
      filter2.matches("pid-3") shouldBe false
    }
  }

}
