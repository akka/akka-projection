/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc

import akka.projection.grpc.consumer.ConsumerFilter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class ConsumerFilterSpec extends AnyWordSpecLike with Matchers {
  import ConsumerFilter._

  "ConsumerFilter" should {
    "merge and remove include" in {
      val filter1 = Vector(ExcludeEntityIds(Set("a")))
      mergeFilter(Nil, filter1) shouldBe filter1
      mergeFilter(filter1, Nil) shouldBe filter1
      val filter2 = Vector(IncludeEntityIds(Set(EntityIdOffset("a", 1))))
      mergeFilter(filter1, filter2) shouldBe filter1 ++ filter2
      mergeFilter(filter2, filter1) shouldBe filter1 ++ filter2
      val filter3 = Vector(RemoveIncludeEntityIds(Set("a")))
      mergeFilter(filter1 ++ filter2, filter3) shouldBe filter1
      mergeFilter(filter3, filter1 ++ filter2) shouldBe filter1
    }

    "merge and reduce filter" in {
      val filter1 =
        Vector(
          ExcludeEntityIds(Set("a", "b", "c")),
          IncludeEntityIds(Set(EntityIdOffset("b", 1), EntityIdOffset("c", 2))))

      val filter2 =
        Vector(ExcludeEntityIds(Set("d")), RemoveIncludeEntityIds(Set("a", "b")), RemoveExcludeEntityIds(Set("c")))

      val expectedFilter =
        Vector(ExcludeEntityIds(Set("a", "b", "d")), IncludeEntityIds(Set(EntityIdOffset("c", 2))))

      mergeFilter(filter1, filter2) shouldBe expectedFilter
      mergeFilter(filter2, filter1) shouldBe expectedFilter
    }

    "merge and use highest seqNr" in {
      val filter1 =
        Vector(IncludeEntityIds(Set(EntityIdOffset("b", 1), EntityIdOffset("b", 2), EntityIdOffset("c", 2))))
      val expectedFilter1 = Vector(IncludeEntityIds(Set(EntityIdOffset("b", 2), EntityIdOffset("c", 2))))
      mergeFilter(Nil, filter1) shouldBe expectedFilter1
      mergeFilter(filter1, Nil) shouldBe expectedFilter1

      val filter2 =
        Vector(IncludeEntityIds(Set(EntityIdOffset("b", 3), EntityIdOffset("c", 1))))
      val expectedFilter2 = Vector(IncludeEntityIds(Set(EntityIdOffset("b", 3), EntityIdOffset("c", 2))))
      mergeFilter(filter1, filter2) shouldBe expectedFilter2
    }

    "create diff for ExcludeRegexEntityIds" in {
      val filter1 = Vector(ExcludeRegexEntityIds(Set("a.*", "b.*")))
      createDiff(Nil, filter1) shouldBe filter1
      createDiff(filter1, Nil) shouldBe Vector(RemoveExcludeRegexEntityIds(Set("a.*", "b.*")))

      val filter2 = Vector(ExcludeRegexEntityIds(Set("a.*", "c.*")))
      createDiff(filter1, filter2) shouldBe Vector(
        ExcludeRegexEntityIds(Set("c.*")),
        RemoveExcludeRegexEntityIds(Set("b.*")))
    }

    "create diff for ExcludeEntityIds" in {
      val filter1 = Vector(ExcludeEntityIds(Set("a", "b")))
      createDiff(Nil, filter1) shouldBe filter1
      createDiff(filter1, Nil) shouldBe Vector(RemoveExcludeEntityIds(Set("a", "b")))

      val filter2 = Vector(ExcludeEntityIds(Set("a", "c")))
      createDiff(filter1, filter2) shouldBe Vector(ExcludeEntityIds(Set("c")), RemoveExcludeEntityIds(Set("b")))
    }

    "create diff for IncludeEntityIds" in {
      val filter1 = Vector(IncludeEntityIds(Set(EntityIdOffset("a", 1), EntityIdOffset("b", 1))))
      createDiff(Nil, filter1) shouldBe filter1
      createDiff(filter1, Nil) shouldBe Vector(RemoveIncludeEntityIds(Set("a", "b")))

      val filter2 = Vector(IncludeEntityIds(Set(EntityIdOffset("a", 1), EntityIdOffset("c", 1))))
      createDiff(filter1, filter2) shouldBe Vector(
        IncludeEntityIds(Set(EntityIdOffset("c", 1))),
        RemoveIncludeEntityIds(Set("b")))
    }

    "create diff for IncludeEntityIds and use highest seqNr" in {
      val filter1 =
        Vector(IncludeEntityIds(Set(EntityIdOffset("a", 1), EntityIdOffset("b", 2), EntityIdOffset("c", 3))))
      val filter2 =
        Vector(IncludeEntityIds(Set(EntityIdOffset("a", 1), EntityIdOffset("b", 3), EntityIdOffset("c", 1))))
      createDiff(filter1, filter2) shouldBe Vector(IncludeEntityIds(Set(EntityIdOffset("b", 3))))
    }
  }

}
