/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.projection.grpc.consumer.ConsumerFilter
import akka.projection.grpc.consumer.ConsumerFilter.ConsumerFilterSettings
import akka.projection.grpc.consumer.ConsumerFilter.CurrentFilter
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

abstract class ConsumerFilterStoreSpec(implName: String, config: Config)
    extends ScalaTestWithActorTestKit(config)
    with AnyWordSpecLike
    with Matchers
    with TestData
    with LogCapturing {

  val consumerFilterSettings = ConsumerFilterSettings(system)

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
          ConsumerFilter.IncludeEntityIds(Set(ConsumerFilter.EntityIdOffset("b", 1))))
      store1 ! ConsumerFilterStore.UpdateFilter(filter)
      notifyProbe.expectMessage(ConsumerFilterRegistry.FilterUpdated(streamId, filter))
      testKit.stop(store1)

      val store2 = spawnStore(streamId)
      store2 ! ConsumerFilterStore.GetFilter(replyProbe.ref)
      replyProbe.expectMessage(CurrentFilter(streamId, filter))
      testKit.stop(store2)
    }

    "update filter by merging with current filter" in {
      val replyProbe = createTestProbe[ConsumerFilter.CurrentFilter]()
      val streamId = nextStreamId()
      val store = spawnStore(streamId)
      val filter1 =
        Vector(
          ConsumerFilter.ExcludeTags(Set("t1", "t2")),
          ConsumerFilter.ExcludeEntityIds(Set("a", "b", "c")),
          ConsumerFilter.IncludeEntityIds(Set(ConsumerFilter.EntityIdOffset("b", 1))))
      store ! ConsumerFilterStore.UpdateFilter(filter1)
      notifyProbe.expectMessage(ConsumerFilterRegistry.FilterUpdated(streamId, filter1))

      val filter2 =
        Vector(
          ConsumerFilter.RemoveExcludeTags(Set("t2")),
          ConsumerFilter.ExcludeEntityIds(Set("d")),
          ConsumerFilter.RemoveExcludeEntityIds(Set("c")),
          ConsumerFilter.RemoveIncludeEntityIds(Set("b")))
      store ! ConsumerFilterStore.UpdateFilter(filter2)
      val expectedFilter =
        Vector(ConsumerFilter.ExcludeTags(Set("t1")), ConsumerFilter.ExcludeEntityIds(Set("a", "b", "d")))
      notifyProbe.expectMessage(ConsumerFilterRegistry.FilterUpdated(streamId, expectedFilter))

      store ! ConsumerFilterStore.GetFilter(replyProbe.ref)
      replyProbe.expectMessage(CurrentFilter(streamId, expectedFilter))

      testKit.stop(store)
    }
  }

}

class LocalConsumerFilterStoreSpec extends ConsumerFilterStoreSpec("LocalConsumerFilterStore", ConfigFactory.empty) {
  override def spawnStore(streamId: String): ActorRef[ConsumerFilterStore.Command] = {
    spawn(LocalConsumerFilterStore(streamId, notifyProbe.ref))
  }
}

class DdataConsumerFilterStoreSpec
    extends ConsumerFilterStoreSpec(
      "DdataConsumerFilterStore",
      ConfigFactory.parseString("""
    akka.actor {
      provider = cluster
      remote.artery {
        canonical.host = "127.0.0.1"
        canonical.port = 0
      }
    }
    """)) {
  override def spawnStore(streamId: String): ActorRef[ConsumerFilterStore.Command] = {
    spawn(DdataConsumerFilterStore(consumerFilterSettings, streamId, notifyProbe.ref))
  }

  Cluster(system).manager ! Join(Cluster(system).selfMember.address)

  "additionally, create instance dynamically due to optional dependency" in {
    val replyProbe = createTestProbe[ConsumerFilter.CurrentFilter]()
    val streamId = nextStreamId()
    ConsumerFilterStore.useDistributedData(system) shouldBe true
    val behv =
      ConsumerFilterStore.createDdataConsumerFilterStore(system, consumerFilterSettings, streamId, notifyProbe.ref)
    val store = spawn(behv)
    store ! ConsumerFilterStore.GetFilter(replyProbe.ref)
    replyProbe.expectMessage(CurrentFilter(streamId, Nil))
    testKit.stop(store)
  }
}
