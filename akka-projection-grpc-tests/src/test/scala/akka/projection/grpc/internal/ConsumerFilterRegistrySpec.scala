/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.projection.grpc.consumer.ConsumerFilter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class ConsumerFilterRegistrySpec
    extends ScalaTestWithActorTestKit
    with AnyWordSpecLike
    with Matchers
    with TestData
    with LogCapturing {

  private var streamIdCounter = 0
  def nextStreamId(): String = {
    streamIdCounter += 1
    s"stream$streamIdCounter"
  }

  "ConsumerFilterRegistry" must {
    "get filter" in {
      val streamId = nextStreamId()
      val registry = spawn(ConsumerFilterRegistry())

      val currentProbe = createTestProbe[ConsumerFilter.CurrentFilter]()
      registry ! ConsumerFilter.GetFilter(streamId, currentProbe.ref)
      currentProbe.expectMessage(ConsumerFilter.CurrentFilter(streamId, Nil))

      val filter1 = Vector(ConsumerFilter.ExcludeEntityIds(Set("a", "c")))
      registry ! ConsumerFilter.UpdateFilter(streamId, filter1)
      registry ! ConsumerFilter.GetFilter(streamId, currentProbe.ref)
      currentProbe.expectMessage(ConsumerFilter.CurrentFilter(streamId, filter1))

      val filter2 =
        Vector(
          ConsumerFilter.ExcludeEntityIds(Set("a", "b")),
          ConsumerFilter.IncludeEntityIds(Set(ConsumerFilter.EntityIdOffset("b", 1))),
          ConsumerFilter.RemoveExcludeEntityIds(Set("c")))

      registry ! ConsumerFilter.UpdateFilter(streamId, filter2)
      registry ! ConsumerFilter.GetFilter(streamId, currentProbe.ref)
      currentProbe.expectMessage(
        ConsumerFilter.CurrentFilter(
          streamId,
          Vector(
            ConsumerFilter.ExcludeEntityIds(Set("a", "b")),
            ConsumerFilter.IncludeEntityIds(Set(ConsumerFilter.EntityIdOffset("b", 1))))))

    }

    "update filter and notify subscriber" in {
      val streamId = nextStreamId()
      val registry = spawn(ConsumerFilterRegistry())

      val subscriberProbe = createTestProbe[ConsumerFilter.SubscriberCommand]()
      registry ! ConsumerFilter.Subscribe(streamId, Nil, subscriberProbe.ref)

      val filter =
        Vector(
          ConsumerFilter.ExcludeEntityIds(Set("a", "b", "c")),
          ConsumerFilter.IncludeEntityIds(Set(ConsumerFilter.EntityIdOffset("b", 1))))
      registry ! ConsumerFilter.UpdateFilter(streamId, filter)

      subscriberProbe.expectMessage(ConsumerFilter.UpdateFilter(streamId, filter))
    }

    "notify subscriber with diff" in {
      val streamId = nextStreamId()
      val registry = spawn(ConsumerFilterRegistry())

      val subscriberProbe1 = createTestProbe[ConsumerFilter.SubscriberCommand]()
      registry ! ConsumerFilter.Subscribe(streamId, Nil, subscriberProbe1.ref)

      val filter1 = Vector(ConsumerFilter.ExcludeEntityIds(Set("a", "c")))
      registry ! ConsumerFilter.UpdateFilter(streamId, filter1)
      subscriberProbe1.expectMessage(ConsumerFilter.UpdateFilter(streamId, filter1))

      val filter2 =
        Vector(
          ConsumerFilter.ExcludeEntityIds(Set("a", "b")),
          ConsumerFilter.IncludeEntityIds(Set(ConsumerFilter.EntityIdOffset("b", 1))),
          ConsumerFilter.RemoveExcludeEntityIds(Set("c")))
      registry ! ConsumerFilter.UpdateFilter(streamId, filter2)

      // subscriber1 receives the diff
      subscriberProbe1.expectMessage(
        ConsumerFilter.UpdateFilter(
          streamId,
          Vector(
            ConsumerFilter.ExcludeEntityIds(Set("b")),
            ConsumerFilter.RemoveExcludeEntityIds(Set("c")),
            ConsumerFilter.IncludeEntityIds(Set(ConsumerFilter.EntityIdOffset("b", 1))))))

      // subscriber2 starts from filter2
      val subscriberProbe2 = createTestProbe[ConsumerFilter.SubscriberCommand]()
      registry ! ConsumerFilter.Subscribe(streamId, filter2, subscriberProbe2.ref)
      subscriberProbe2.expectNoMessage()

      val filter3 = Vector(ConsumerFilter.ExcludeEntityIds(Set("d")))
      registry ! ConsumerFilter.UpdateFilter(streamId, filter3)
      // diff is sent to both subscribers
      subscriberProbe1.expectMessage(ConsumerFilter.UpdateFilter(streamId, filter3))
      subscriberProbe2.expectMessage(ConsumerFilter.UpdateFilter(streamId, filter3))
    }

    "start diff from init filter" in {
      val streamId = nextStreamId()
      val registry = spawn(ConsumerFilterRegistry())

      val initFilter = Vector(ConsumerFilter.ExcludeEntityIds(Set("a", "c")))
      registry ! ConsumerFilter.UpdateFilter(streamId, initFilter)

      // before registering the subscriber it would retrieve the init filter
      val currentProbe = createTestProbe[ConsumerFilter.CurrentFilter]()
      registry ! ConsumerFilter.GetFilter(streamId, currentProbe.ref)
      currentProbe.expectMessage(ConsumerFilter.CurrentFilter(streamId, initFilter))

      // that init filter is then included in the Subscribe, to be used as starting point for evaluating
      // the diff for that subscriber
      val subscriberProbe = createTestProbe[ConsumerFilter.SubscriberCommand]()
      registry ! ConsumerFilter.Subscribe(streamId, initFilter, subscriberProbe.ref)

      val filter2 =
        Vector(
          ConsumerFilter.ExcludeEntityIds(Set("a", "b")),
          ConsumerFilter.IncludeEntityIds(Set(ConsumerFilter.EntityIdOffset("b", 1))),
          ConsumerFilter.RemoveExcludeEntityIds(Set("c")))
      registry ! ConsumerFilter.UpdateFilter(streamId, filter2)

      // diff between filter2 and initFilter
      subscriberProbe.expectMessage(
        ConsumerFilter.UpdateFilter(
          streamId,
          Vector(
            ConsumerFilter.ExcludeEntityIds(Set("b")),
            ConsumerFilter.RemoveExcludeEntityIds(Set("c")),
            ConsumerFilter.IncludeEntityIds(Set(ConsumerFilter.EntityIdOffset("b", 1))))))
    }

    "notify new subscriber of diff from concurrent update" in {
      val streamId = nextStreamId()
      val registry = spawn(ConsumerFilterRegistry())

      val initFilter = Vector(ConsumerFilter.ExcludeEntityIds(Set("a", "c")))
      registry ! ConsumerFilter.UpdateFilter(streamId, initFilter)

      // before registering the subscriber it would retrieve the init filter
      val currentProbe = createTestProbe[ConsumerFilter.CurrentFilter]()
      registry ! ConsumerFilter.GetFilter(streamId, currentProbe.ref)
      currentProbe.expectMessage(ConsumerFilter.CurrentFilter(streamId, initFilter))

      // but it could be updated before it registers
      val filter2 =
        Vector(
          ConsumerFilter.ExcludeEntityIds(Set("a", "b")),
          ConsumerFilter.IncludeEntityIds(Set(ConsumerFilter.EntityIdOffset("b", 1))),
          ConsumerFilter.RemoveExcludeEntityIds(Set("c")))
      registry ! ConsumerFilter.UpdateFilter(streamId, filter2)

      // let the update complete before registering the subscriber, but it will work otherwise as well
      Thread.sleep(1000)

      val subscriberProbe = createTestProbe[ConsumerFilter.SubscriberCommand]()
      registry ! ConsumerFilter.Subscribe(streamId, initFilter, subscriberProbe.ref)

      // diff between filter2 and initFilter
      subscriberProbe.expectMessage(
        ConsumerFilter.UpdateFilter(
          streamId,
          Vector(
            ConsumerFilter.ExcludeEntityIds(Set("b")),
            ConsumerFilter.RemoveExcludeEntityIds(Set("c")),
            ConsumerFilter.IncludeEntityIds(Set(ConsumerFilter.EntityIdOffset("b", 1))))))
    }
  }

}
