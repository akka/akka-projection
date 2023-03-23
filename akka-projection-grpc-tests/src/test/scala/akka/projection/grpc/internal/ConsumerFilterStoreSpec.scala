/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.internal

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import akka.projection.grpc.consumer.ConsumerFilter
import akka.projection.grpc.consumer.ConsumerFilter.CurrentFilter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

abstract class ConsumerFilterStoreSpec(implName: String)
    extends ScalaTestWithActorTestKit
    with AnyWordSpecLike
    with Matchers
    with TestData
    with LogCapturing {

  val notifyProbe = createTestProbe[ConsumerFilterRegistry.FilterUpdated]()

  private var streamIdCounter = 0
  def nextStreamId(): String = {
    streamIdCounter += 1
    s"stream$streamIdCounter"
  }

  def spawnStore(streamId: String): ActorRef[ConsumerFilterStore.Command]

  implName must {
    "get empty filter" in {
      val replyProbe = createTestProbe[ConsumerFilter.CurrentFilter]()
      val streamId = nextStreamId()
      val store = spawnStore(streamId)
      store ! ConsumerFilterStore.GetFilter(replyProbe.ref)
      replyProbe.expectMessage(CurrentFilter(streamId, Nil))
      testKit.stop(store)
    }

    "update filter and keep filter state when spawning new store" in {
      val replyProbe = createTestProbe[ConsumerFilter.CurrentFilter]()
      val streamId = nextStreamId()
      val store1 = spawnStore(streamId)
      val filter =
        Vector(
          ConsumerFilter.ExcludeEntityIds(Set("a", "b", "c")),
          ConsumerFilter.IncludeEntityIds(
            Set(ConsumerFilter.EntityIdOffset("b", 1), ConsumerFilter.EntityIdOffset("b", 2))))
      store1 ! ConsumerFilterStore.UpdateFilter(filter)
      notifyProbe.expectMessage(ConsumerFilterRegistry.FilterUpdated(streamId, filter))
      testKit.stop(store1)

      val store2 = spawnStore(streamId)
      store2 ! ConsumerFilterStore.GetFilter(replyProbe.ref)
      replyProbe.expectMessage(CurrentFilter(streamId, filter))
      testKit.stop(store2)
    }
  }

}

class LocalConsumerFilterStoreSpec extends ConsumerFilterStoreSpec("LocalConsumerFilterStore") {
  override def spawnStore(streamId: String): ActorRef[ConsumerFilterStore.Command] = {
    spawn(Behaviors.setup[ConsumerFilterStore.Command] { context =>
      new LocalConsumerFilterStore(context, streamId, notifyProbe.ref).behavior()
    })
  }

  "additionally, LocalConsumerFilterStore" must {
    "reduce filter" in {
      val filter1 =
        Vector(
          ConsumerFilter.ExcludeEntityIds(Set("a", "b", "c")),
          ConsumerFilter.IncludeEntityIds(
            Set(ConsumerFilter.EntityIdOffset("b", 1), ConsumerFilter.EntityIdOffset("c", 2))))

      val filter2 =
        Vector(
          ConsumerFilter.ExcludeEntityIds(Set("d")),
          ConsumerFilter.RemoveIncludeEntityIds(Set("a", "b")),
          ConsumerFilter.RemoveExcludeEntityIds(Set("c")))

      val expectedFilter =
        Vector(
          ConsumerFilter.ExcludeEntityIds(Set("a", "b")),
          ConsumerFilter.IncludeEntityIds(Set(ConsumerFilter.EntityIdOffset("c", 2))),
          ConsumerFilter.ExcludeEntityIds(Set("d")))

      LocalConsumerFilterStore.reduceFilter(filter1, filter2) shouldBe expectedFilter
    }
  }

}
