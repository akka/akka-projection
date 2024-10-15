/*
 * Copyright (C) 2020-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.UniqueAddress
import akka.cluster.ddata.SelfUniqueAddress
import akka.projection.grpc.consumer.ConsumerFilter
import akka.serialization.SerializationExtension
import org.scalatest.wordspec.AnyWordSpecLike

class ConsumerSerializerSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  private val classicSystem = system.toClassic
  private val serializer = new ConsumerSerializer(classicSystem.asInstanceOf[ExtendedActorSystem])

  private val node1 = SelfUniqueAddress(UniqueAddress(Address("akka", system.name, "node1", 2552), 1L))
  private val node2 = SelfUniqueAddress(UniqueAddress(Address("akka", system.name, "node2", 2552), 2L))

  private val filter1 =
    Vector(
      ConsumerFilter.ExcludeTags(Set("t1", "t2")),
      ConsumerFilter.IncludeTags(Set("t3", "t4")),
      ConsumerFilter.IncludeTopics(Set("a/+/b", "c/#")),
      ConsumerFilter.ExcludeRegexEntityIds(Set("all.*")),
      ConsumerFilter.IncludeRegexEntityIds(Set(".*akka.*")),
      ConsumerFilter.ExcludeEntityIds(Set("a", "b", "c")),
      ConsumerFilter.IncludeEntityIds(Set(ConsumerFilter.EntityIdOffset("b", 1))))

  private val filter2 =
    Vector(
      ConsumerFilter.RemoveExcludeTags(Set("t2")),
      ConsumerFilter.RemoveIncludeTags(Set("t4")),
      ConsumerFilter.ExcludeEntityIds(Set("d")),
      ConsumerFilter.RemoveExcludeEntityIds(Set("c")),
      ConsumerFilter.RemoveIncludeEntityIds(Set("b")),
      ConsumerFilter.RemoveExcludeRegexEntityIds(Set("all.*")))

  private val filter3 =
    Vector(ConsumerFilter.IncludeEntityIds(Set(ConsumerFilter.EntityIdOffset("b", 1))))

  private val filter4 =
    Vector(ConsumerFilter.RemoveIncludeEntityIds(Set("b")))

  "ConsumerSerializer" must {

    Seq(
      "empty ConsumerFilterStore.State" -> DdataConsumerFilterStore.State.empty,
      "ConsumerFilterStore.State with filter" -> DdataConsumerFilterStore.State.empty.updated(filter1)(node1),
      "ConsumerFilterStore.State with filter updated from two nodes" -> DdataConsumerFilterStore.State.empty
        .updated(filter1)(node1)
        .updated(filter2)(node2),
      "empty after updates ConsumerFilterStore.State" -> DdataConsumerFilterStore.State.empty
        .updated(filter3)(node1)
        .updated(filter4)(node1),
      "ConsumerFilterKey" -> DdataConsumerFilterStore.ConsumerFilterKey("abc")).foreach {
      case (scenario, item) =>
        s"resolve serializer for $scenario" in {
          val serializer = SerializationExtension(classicSystem)
          serializer.serializerFor(item.getClass).getClass should be(classOf[ConsumerSerializer])
        }

        s"serialize and de-serialize $scenario" in {
          verifySerialization(item)
        }
    }
  }

  def verifySerialization(msg: AnyRef): Unit = {
    serializer.fromBinary(serializer.toBinary(msg), serializer.manifest(msg)) should be(msg)
  }

}
