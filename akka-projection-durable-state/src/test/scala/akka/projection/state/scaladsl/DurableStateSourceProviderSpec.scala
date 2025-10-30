/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.state.scaladsl

import scala.concurrent.Future

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import akka.actor.testkit.typed.scaladsl._
import akka.persistence.query.Offset
import akka.persistence.query.UpdatedDurableState
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.state.scaladsl._
import akka.persistence.testkit.state.scaladsl.PersistenceTestKitDurableStateStore
import akka.stream.testkit.scaladsl.TestSink
import akka.persistence.testkit.PersistenceTestKitDurableStateStorePlugin

object DurableStateSourceProviderSpec {
  def conf: Config = PersistenceTestKitDurableStateStorePlugin.config.withFallback(ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    """))
  final case class Record(id: Int, name: String)
}

class DurableStateSourceProviderSpec
    extends ScalaTestWithActorTestKit(DurableStateSourceProviderSpec.conf)
    with AnyWordSpecLike {
  "A DurableStateSourceProvider" must {
    import DurableStateSourceProviderSpec._

    "provide changes by tag" in {
      val record = Record(0, "Name-1")
      val tag = "tag-a"
      val recordChange = Record(0, "Name-2")

      val durableStateStore: DurableStateUpdateStore[Record] =
        DurableStateStoreRegistry(system)
          .durableStateStoreFor[DurableStateUpdateStore[Record]](PersistenceTestKitDurableStateStore.Identifier)
      implicit val ec = system.classicSystem.dispatcher
      val fut = Future.sequence(
        Vector(
          durableStateStore.upsertObject("persistent-id-1", 0L, record, tag),
          durableStateStore.upsertObject("persistent-id-2", 0L, record, "tag-b"),
          durableStateStore.upsertObject("persistent-id-1", 1L, recordChange, tag),
          durableStateStore.upsertObject("persistent-id-3", 0L, record, "tag-c")))
      whenReady(fut) { _ =>
        val sourceProvider =
          DurableStateSourceProvider.changesByTag[Record](system, PersistenceTestKitDurableStateStore.Identifier, tag)

        whenReady(sourceProvider.source(() => Future.successful[Option[Offset]](None))) { source =>
          val stateChange = source
            .collect { case u: UpdatedDurableState[Record] => u }
            .runWith(TestSink[UpdatedDurableState[Record]]())
            .request(1)
            .expectNext()

          stateChange.value should be(recordChange)
          stateChange.revision should be(1L)
        }
      }
    }
  }

}
